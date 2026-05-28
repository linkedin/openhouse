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

// Runs PostCommitOperations after a successful Iceberg commit.
//
// Each operation receives a wall-clock timeout from tables.postcommit.per-op-timeout-ms. Any
// error the operation signals is recorded as a metric and a log line, then swallowed. Dispatch
// is fire-and-forget, so the commit thread is never blocked on operation work.
//
// Operations describe payload and endpoint only. The timeout, error swallowing, subscription,
// and metric emission all live here, so the contract across operations stays uniform.
//
// The bean is constructed only when tables.postcommit.enabled=true.
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

  // Dispatches all registered operations for savedDto.
  //
  // Returns immediately on the calling thread. Each operation runs on its underlying reactive
  // scheduler. This method never throws.
  public void dispatch(TableDto savedDto) {
    for (PostCommitOperation op : operations) {
      decorate(op, savedDto).ifPresent(Mono::subscribe);
    }
  }

  // Returns the fully-decorated Mono for op without subscribing to it.
  //
  // The decoration applies the per-op timeout, records the success or error metric, and swallows
  // any error. When the operation does not apply (or its prepare() throws synchronously), this
  // method emits the "skipped" or "prepare_threw" metric and returns Optional.empty().
  //
  // Package-private so that tests can .block() on the chain rather than poll for metric emission
  // after a fire-and-forget subscription.
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

  // Maps a terminal error to a small set of outcome tags. The classifier lives here so that all
  // operations share the same taxonomy.
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
