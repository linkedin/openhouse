package com.linkedin.openhouse.internal.catalog;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PostCommitOperationRunnerTest {

  private static final PostCommitContext CONTEXT =
      new PostCommitContext(TableIdentifier.of("db", "tbl"), Mockito.mock(TableMetadata.class));

  private static PostCommitOperationRunner newRunner(
      List<PostCommitOperation> ops, SimpleMeterRegistry registry) {
    return new PostCommitOperationRunner(ops, registry, true, 4, 1000, 30, true, 2000);
  }

  /**
   * Returns the counter value once it becomes visible. Metrics are recorded by the runner's worker
   * threads after the operation body completes, so tests must poll rather than read once.
   */
  private static double awaitCounter(SimpleMeterRegistry registry, String name, String result)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5000;
    Counter counter = null;
    while (System.currentTimeMillis() < deadline) {
      counter =
          registry
              .find(PostCommitOperationRunner.METRIC_PREFIX)
              .tag(PostCommitOperationRunner.TAG_NAME, name)
              .tag(PostCommitOperationRunner.TAG_RESULT, result)
              .counter();
      if (counter != null && counter.count() > 0) {
        return counter.count();
      }
      Thread.sleep(10);
    }
    return counter == null ? 0.0 : counter.count();
  }

  /** Every registered operation runs, and success is recorded per operation name. */
  @Test
  void testRunsAllOperations() throws InterruptedException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    PostCommitOperation opA = latchOp("op-a", null);
    PostCommitOperation opB = latchOp("op-b", null);

    newRunner(Arrays.asList(opA, opB), registry).runAll(CONTEXT);

    Assertions.assertEquals(1.0, awaitCounter(registry, "op-a", "success"));
    Assertions.assertEquals(1.0, awaitCounter(registry, "op-b", "success"));
  }

  /** A throwing operation is isolated: other operations still run and nothing propagates. */
  @Test
  void testFailureIsIsolated() throws InterruptedException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    PostCommitOperation good = latchOp("good", null);
    PostCommitOperation bad = latchOp("bad", new RuntimeException("boom"));

    Assertions.assertDoesNotThrow(
        () -> newRunner(Arrays.asList(good, bad), registry).runAll(CONTEXT));

    Assertions.assertEquals(1.0, awaitCounter(registry, "good", "success"));
    Assertions.assertEquals(1.0, awaitCounter(registry, "bad", "failed"));
  }

  /** A slow operation exceeding the timeout is interrupted and recorded as a timeout. */
  @Test
  void testTimeoutInterruptsSlowOperation() throws InterruptedException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    AtomicBoolean interrupted = new AtomicBoolean(false);
    CountDownLatch done = new CountDownLatch(1);
    PostCommitOperation slow =
        new PostCommitOperation() {
          @Override
          public String getName() {
            return "slow";
          }

          @Override
          public void execute(PostCommitContext context) throws Exception {
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              interrupted.set(true);
              Thread.currentThread().interrupt();
              throw e;
            } finally {
              done.countDown();
            }
          }
        };

    PostCommitOperationRunner runner =
        new PostCommitOperationRunner(
            Collections.singletonList(slow), registry, true, 4, 1000, 30, true, /*timeoutMs*/ 100);
    runner.runAll(CONTEXT);

    Assertions.assertTrue(done.await(5, TimeUnit.SECONDS), "slow op should have been interrupted");
    Assertions.assertTrue(interrupted.get(), "op thread should observe interruption");
    Assertions.assertEquals(1.0, awaitCounter(registry, "slow", "timeout"));
  }

  /** Null context and empty operation list are safe no-ops. */
  @Test
  void testNoOpWhenNothingToRun() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    Assertions.assertDoesNotThrow(
        () -> {
          newRunner(Collections.emptyList(), registry).runAll(CONTEXT);
          newRunner(Collections.singletonList(latchOp("x", null)), registry).runAll(null);
        });
  }

  /**
   * Saturating a single-thread/single-slot pool drops overflow work as rejected, never blocking.
   */
  @Test
  void testRejectionWhenPoolSaturated() throws InterruptedException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger started = new AtomicInteger();
    // Block the single worker and the single queue slot so remaining submissions are rejected.
    PostCommitOperation blocking =
        new PostCommitOperation() {
          @Override
          public String getName() {
            return "blocking";
          }

          @Override
          public void execute(PostCommitContext context) throws Exception {
            started.incrementAndGet();
            release.await(5, TimeUnit.SECONDS);
          }
        };

    // maxThreads=1; note queue-capacity is floored to 100, so saturating the pool requires more
    // than 1 (running) + 100 (queued) ops. High timeout so cancellation doesn't interfere.
    PostCommitOperationRunner runner =
        new PostCommitOperationRunner(
            Collections.nCopies(150, blocking), registry, true, 1, 1, 30, true, 60000);
    runner.runAll(CONTEXT);
    release.countDown();

    double rejected = awaitCounter(registry, "blocking", "rejected");
    Assertions.assertTrue(rejected >= 1.0, "at least one submission should be rejected");
  }

  /** When disabled via server config, the runner never dispatches operations. */
  @Test
  void testDisabledRunnerIsNoOp() throws InterruptedException {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    AtomicInteger ran = new AtomicInteger();
    PostCommitOperation op =
        new PostCommitOperation() {
          @Override
          public String getName() {
            return "disabled-op";
          }

          @Override
          public void execute(PostCommitContext context) {
            ran.incrementAndGet();
          }
        };

    PostCommitOperationRunner runner =
        new PostCommitOperationRunner(
            Collections.singletonList(op), registry, /*enabled*/ false, 4, 1000, 30, true, 10000);
    Assertions.assertFalse(runner.isEnabled());
    runner.runAll(CONTEXT);

    Thread.sleep(200);
    Assertions.assertEquals(0, ran.get(), "disabled runner must not execute operations");
    Assertions.assertNull(
        registry.find(PostCommitOperationRunner.METRIC_PREFIX).counter(),
        "disabled runner must not emit operation metrics");
  }

  private static PostCommitOperation latchOp(String name, RuntimeException toThrow) {
    return new PostCommitOperation() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public void execute(PostCommitContext context) {
        if (toThrow != null) {
          throw toThrow;
        }
      }
    };
  }
}
