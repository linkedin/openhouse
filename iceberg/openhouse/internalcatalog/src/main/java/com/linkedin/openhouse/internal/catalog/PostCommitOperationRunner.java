package com.linkedin.openhouse.internal.catalog;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Runs {@link PostCommitOperation}s after a successful commit with bounded, best-effort guarantees.
 *
 * <p>This is pure infrastructure: it knows nothing about what any operation does. It owns the
 * execution safety envelope so that arbitrary post-commit business logic can never destabilize the
 * commit path:
 *
 * <ul>
 *   <li><b>Bounded concurrency &amp; memory.</b> A fixed-size thread pool with a bounded queue.
 *       When the queue is full, work is dropped ({@link
 *       java.util.concurrent.ThreadPoolExecutor.AbortPolicy}) rather than blocking the committer or
 *       growing unbounded. Threads are daemon, named, and time out when idle so an unused runner
 *       holds zero threads.
 *   <li><b>Hard per-operation timeout.</b> A timeout scheduler cancels/interrupts any operation
 *       exceeding the configured timeout, so a single slow/hung op (e.g. an OOM-inducing or
 *       network-stuck call) cannot pin a worker forever.
 *   <li><b>Failure isolation.</b> Each operation runs independently; a throwing op does not affect
 *       others and never propagates to the committer.
 *   <li><b>Uniform observability.</b> Submitted / success / failed / timeout / rejected counters
 *       are emitted, tagged by operation name.
 * </ul>
 *
 * <p>In OSS/dev deployments no {@link PostCommitOperation} beans exist, so {@link #runAll} is a
 * no-op.
 */
@Slf4j
@Component
public class PostCommitOperationRunner {

  static final String METRIC_PREFIX = "openhouse_postcommit_operation";
  static final String TAG_NAME = "name";
  static final String TAG_RESULT = "result";

  private final List<PostCommitOperation> operations;
  private final MeterRegistry meterRegistry;
  private final ThreadPoolExecutor executor;
  private final ScheduledExecutorService timeoutScheduler;
  private final long operationTimeoutMs;

  @Autowired
  public PostCommitOperationRunner(
      List<PostCommitOperation> operations,
      MeterRegistry meterRegistry,
      @Value("${cluster.tables.postcommit.max-threads:4}") int maxThreads,
      @Value("${cluster.tables.postcommit.queue-capacity:1000}") int queueCapacity,
      @Value("${cluster.tables.postcommit.operation-timeout-ms:10000}") long operationTimeoutMs) {
    this.operations = operations == null ? Collections.emptyList() : operations;
    this.meterRegistry = meterRegistry;
    this.operationTimeoutMs = Math.max(1L, operationTimeoutMs);

    int poolSize = Math.max(1, maxThreads);
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(Math.max(1, queueCapacity)),
            daemonThreadFactory("post-commit-op"),
            new ThreadPoolExecutor.AbortPolicy());
    // Let idle workers (and the timeout scheduler) go away so an unused runner holds zero threads.
    tpe.allowCoreThreadTimeOut(true);
    this.executor = tpe;
    this.timeoutScheduler =
        Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("post-commit-timeout"));

    log.info(
        "PostCommitOperationRunner initialized with {} operation(s): {} (maxThreads={}, queueCapacity={}, operationTimeoutMs={})",
        this.operations.size(),
        operationNames(),
        poolSize,
        queueCapacity,
        this.operationTimeoutMs);
  }

  /**
   * Submits every registered operation for asynchronous, best-effort execution. Returns
   * immediately; never throws.
   *
   * @param context committed table context; ignored if {@code null}
   */
  public void runAll(PostCommitContext context) {
    if (context == null || operations.isEmpty()) {
      return;
    }
    for (PostCommitOperation operation : operations) {
      submit(operation, context);
    }
  }

  private void submit(PostCommitOperation operation, PostCommitContext context) {
    final String name = operation.getName();
    try {
      Future<?> future = executor.submit(() -> execute(operation, context));
      count(name, "submitted");
      timeoutScheduler.schedule(
          () -> {
            // cancel(true) returns true only if the task had not yet completed; it interrupts a
            // running worker so a hung operation cannot pin the thread.
            if (future.cancel(true)) {
              count(name, "timeout");
              log.warn(
                  "Post-commit operation '{}' timed out after {} ms for table {}",
                  name,
                  operationTimeoutMs,
                  context.getTableIdentifier());
            }
          },
          operationTimeoutMs,
          TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException rejected) {
      // Queue full: drop rather than block the committer or grow memory unbounded.
      count(name, "rejected");
      log.warn(
          "Post-commit operation '{}' rejected (pool saturated) for table {}",
          name,
          context.getTableIdentifier());
    } catch (Throwable submitFailure) {
      count(name, "rejected");
      log.warn("Failed to submit post-commit operation '{}'", name, submitFailure);
    }
  }

  private void execute(PostCommitOperation operation, PostCommitContext context) {
    final String name = operation.getName();
    try {
      operation.execute(context);
      count(name, "success");
    } catch (InterruptedException interrupted) {
      // Cancelled by the timeout scheduler (timeout already recorded there); restore the flag and
      // stop.
      Thread.currentThread().interrupt();
    } catch (Throwable failure) {
      if (Thread.currentThread().isInterrupted()) {
        // Interruption surfaced as a wrapped exception; treat as timeout, already counted.
        return;
      }
      count(name, "failed");
      log.warn(
          "Post-commit operation '{}' failed for table {}",
          name,
          context.getTableIdentifier(),
          failure);
    }
  }

  private void count(String name, String result) {
    try {
      meterRegistry.counter(METRIC_PREFIX, TAG_NAME, name, TAG_RESULT, result).increment();
    } catch (Throwable ignored) {
      // Metrics must never break best-effort execution.
    }
  }

  private String operationNames() {
    StringBuilder sb = new StringBuilder();
    for (PostCommitOperation operation : operations) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(operation.getName());
    }
    return sb.toString();
  }

  private static ThreadFactory daemonThreadFactory(String prefix) {
    final AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  @PreDestroy
  void shutdown() {
    executor.shutdownNow();
    timeoutScheduler.shutdownNow();
  }
}
