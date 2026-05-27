package com.linkedin.openhouse.tables.services.postcommit;

import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.model.TableDto;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * Runs {@link PostCommitOperation}s after a successful Iceberg commit. Best-effort and bounded:
 * each operation gets a wall-clock timeout, errors are swallowed after metric/log, and dispatch
 * itself is fire-and-forget so the commit thread is never blocked on operation work.
 *
 * <p><b>Why async.</b> A synchronous post-commit hook converts a downstream outage (the optimizer
 * being slow or unavailable, a network glitch) into a Tables-Service write outage — the post-commit
 * push is a best-effort scheduling signal, not a write-correctness step, and its blast radius must
 * not include the write path. The crash-loss window (a JVM dying after commit and before the HTTP
 * push completes) is acceptable because operations are designed to be cumulative: the next commit
 * carries the same state forward and the consumer self-corrects from missing data.
 *
 * <p><b>Why the dispatcher owns timeouts.</b> Operations only describe payload + endpoint. The
 * timeout/swallow/subscribe machinery lives here once so individual operations stay small and so
 * the contract across operations is uniform (one knob: {@code
 * tables.postcommit.per-op-timeout-ms}).
 *
 * <p>Bean is only wired when {@code tables.postcommit.enabled=true}.
 */
@Slf4j
@Component
@EnableConfigurationProperties(PostCommitProperties.class)
@ConditionalOnProperty(prefix = "tables.postcommit", name = "enabled", havingValue = "true")
public class PostCommitDispatcher {

  private final List<PostCommitOperation> operations;
  private final PostCommitProperties properties;
  private final MeterRegistry meterRegistry;

  public PostCommitDispatcher(
      List<PostCommitOperation> operations,
      PostCommitProperties properties,
      MeterRegistry meterRegistry) {
    this.operations = operations;
    this.properties = properties;
    this.meterRegistry = meterRegistry;
  }

  /**
   * Dispatch all registered operations for {@code savedDto}. Returns immediately on the calling
   * thread; each operation runs on its underlying reactive scheduler. Never throws.
   */
  public void dispatch(TableDto savedDto) {
    for (PostCommitOperation op : operations) {
      decorate(op, savedDto).ifPresent(Mono::subscribe);
    }
  }

  /**
   * Returns the fully-decorated {@link Mono} for {@code op} (per-op timeout, success / error metric
   * emission, error swallow) without subscribing. Emits the {@code skipped} or {@code
   * prepare_threw} metric synchronously and returns {@link Optional#empty()} in those cases.
   *
   * <p>Package-private so tests can {@code .block()} on the decorated chain rather than polling for
   * metric emission after a fire-and-forget {@code subscribe()}.
   */
  Optional<Mono<Void>> decorate(PostCommitOperation op, TableDto savedDto) {
    Optional<Mono<Void>> work;
    try {
      work = op.prepare(savedDto);
    } catch (RuntimeException e) {
      // Defensive: a prepare() that throws synchronously must not break dispatch of later ops.
      meterRegistry
          .counter(
              MetricsConstant.POSTCOMMIT_OP_FAILED, "op", op.name(), "outcome", "prepare_threw")
          .increment();
      log.warn("Post-commit op {} prepare() threw {}", op.name(), e.toString());
      return Optional.empty();
    }
    if (!work.isPresent()) {
      meterRegistry.counter(MetricsConstant.POSTCOMMIT_OP_SKIPPED, "op", op.name()).increment();
      return Optional.empty();
    }
    Timer.Sample sample = Timer.start(meterRegistry);
    Mono<Void> decorated =
        work.get()
            .timeout(Duration.ofMillis(properties.getPerOpTimeoutMs()))
            .doOnSuccess(
                ignored ->
                    sample.stop(
                        meterRegistry.timer(
                            MetricsConstant.POSTCOMMIT_OP_DURATION,
                            "op",
                            op.name(),
                            "outcome",
                            "success")))
            .onErrorResume(
                e -> {
                  String outcome = classifyOutcome(e);
                  sample.stop(
                      meterRegistry.timer(
                          MetricsConstant.POSTCOMMIT_OP_DURATION,
                          "op",
                          op.name(),
                          "outcome",
                          outcome));
                  meterRegistry
                      .counter(
                          MetricsConstant.POSTCOMMIT_OP_FAILED, "op", op.name(), "outcome", outcome)
                      .increment();
                  log.warn("Post-commit op {} failed ({}): {}", op.name(), outcome, e.toString());
                  return Mono.empty();
                });
    return Optional.of(decorated);
  }

  /**
   * Map a terminal error to a small set of outcome tags. Kept here so all operations share the same
   * taxonomy.
   */
  private static String classifyOutcome(Throwable e) {
    if (e instanceof TimeoutException) {
      return "timeout";
    }
    if (e instanceof WebClientRequestException) {
      return "network_error";
    }
    if (e instanceof WebClientResponseException) {
      int code = ((WebClientResponseException) e).getStatusCode().value();
      if (code >= 500 && code < 600) {
        return "server_error";
      }
      if (code >= 400 && code < 500) {
        return "client_error";
      }
    }
    return "unknown_error";
  }
}
