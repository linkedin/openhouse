package com.linkedin.openhouse.tables.services.postcommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.model.TableDto;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class PostCommitDispatcherTest {

  private static final Duration BLOCK_MAX = Duration.ofSeconds(5);

  private MeterRegistry meterRegistry;
  private PostCommitProperties properties;
  private final TableDto savedDto = TableDto.builder().databaseId("db").tableId("t").build();

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    properties = new PostCommitProperties();
    properties.setEnabled(true);
    properties.setPerOpTimeoutMs(500L);
  }

  private PostCommitDispatcher dispatcherWith(PostCommitOperation... ops) {
    return new PostCommitDispatcher(Arrays.asList(ops), properties, meterRegistry);
  }

  @Test
  void dispatch_invokesEveryRegisteredOperationsPrepare() {
    AtomicInteger aCount = new AtomicInteger();
    AtomicInteger bCount = new AtomicInteger();
    dispatcherWith(new EmptyOp("a", aCount), new EmptyOp("b", bCount)).dispatch(savedDto);

    assertThat(aCount.get()).isEqualTo(1);
    assertThat(bCount.get()).isEqualTo(1);
  }

  @Test
  void decorate_emptyPrepare_incrementsSkippedAndReturnsEmpty() {
    PostCommitOperation op = new EmptyOp("opx", new AtomicInteger());

    Optional<Mono<Void>> decorated = dispatcherWith(op).decorate(op, savedDto);

    assertThat(decorated).isEmpty();
    assertThat(meterRegistry.counter(MetricsConstant.POSTCOMMIT_OP_SKIPPED, "op", "opx").count())
        .isEqualTo(1.0);
  }

  @Test
  void decorate_successfulWork_recordsDurationWithSuccessOutcome() {
    PostCommitOperation op = new SimpleOp("ok", Mono::empty);

    dispatcherWith(op).decorate(op, savedDto).orElseThrow(AssertionError::new).block(BLOCK_MAX);

    assertThat(
            meterRegistry
                .timer(MetricsConstant.POSTCOMMIT_OP_DURATION, "op", "ok", "outcome", "success")
                .count())
        .isEqualTo(1L);
  }

  @Test
  void decorate_workSignalsError_incrementsFailedWithClassifiedOutcomeAndSwallowsError() {
    RuntimeException boom = new RuntimeException("boom");
    PostCommitOperation op = new SimpleOp("broke", () -> Mono.error(boom));

    // Dispatcher swallows the error — block() returns normally.
    dispatcherWith(op).decorate(op, savedDto).orElseThrow(AssertionError::new).block(BLOCK_MAX);

    assertThat(
            meterRegistry
                .counter(
                    MetricsConstant.POSTCOMMIT_OP_FAILED, "op", "broke", "outcome", "unknown_error")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void decorate_prepareThrowsSynchronously_incrementsFailedAndDispatchContinuesToLaterOps() {
    AtomicInteger laterCount = new AtomicInteger();
    PostCommitOperation thrower =
        new PostCommitOperation() {
          @Override
          public String name() {
            return "thrower";
          }

          @Override
          public Optional<Mono<Void>> prepare(TableDto dto) {
            throw new IllegalStateException("nope");
          }
        };
    PostCommitOperation later = new EmptyOp("later", laterCount);

    dispatcherWith(thrower, later).dispatch(savedDto);

    assertThat(
            meterRegistry
                .counter(
                    MetricsConstant.POSTCOMMIT_OP_FAILED,
                    "op",
                    "thrower",
                    "outcome",
                    "prepare_threw")
                .count())
        .isEqualTo(1.0);
    assertThat(laterCount.get())
        .as("later operations must still run after a synchronous prepare() throw")
        .isEqualTo(1);
  }

  @Test
  void decorate_workExceedsPerOpTimeout_incrementsFailedWithTimeoutOutcome() {
    properties.setPerOpTimeoutMs(50L);
    PostCommitOperation op = new SimpleOp("slow", Mono::never);

    dispatcherWith(op).decorate(op, savedDto).orElseThrow(AssertionError::new).block(BLOCK_MAX);

    assertThat(
            meterRegistry
                .counter(MetricsConstant.POSTCOMMIT_OP_FAILED, "op", "slow", "outcome", "timeout")
                .count())
        .isEqualTo(1.0);
  }

  // ---- helpers ----

  private static class EmptyOp implements PostCommitOperation {
    private final String name;
    private final AtomicInteger calls;

    EmptyOp(String name, AtomicInteger calls) {
      this.name = name;
      this.calls = calls;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Optional<Mono<Void>> prepare(TableDto dto) {
      calls.incrementAndGet();
      return Optional.empty();
    }
  }

  private static class SimpleOp implements PostCommitOperation {
    private final String name;
    private final java.util.function.Supplier<Mono<Void>> work;

    SimpleOp(String name, java.util.function.Supplier<Mono<Void>> work) {
      this.name = name;
      this.work = work;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Optional<Mono<Void>> prepare(TableDto dto) {
      return Optional.of(work.get());
    }
  }
}
