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
  private final boolean enabled;

  @Autowired
  public PostCommitOperationRunner(
      List<PostCommitOperation> operations,
      MeterRegistry meterRegistry,
      @Value("${cluster.tables.postcommit.enabled:true}") boolean enabled,
      @Value("${cluster.tables.postcommit.max-threads:4}") int maxThreads,
      @Value("${cluster.tables.postcommit.queue-capacity:1000}") int queueCapacity,
      @Value("${cluster.tables.postcommit.idle-thread-keepalive-seconds:30}")
          long idleThreadKeepAliveSeconds,
      @Value("${cluster.tables.postcommit.reclaim-idle-threads:true}") boolean reclaimIdleThreads,
      @Value("${cluster.tables.postcommit.operation-timeout-ms:10000}") long operationTimeoutMs) {
    this.operations = operations == null ? Collections.emptyList() : operations;
    this.meterRegistry = meterRegistry;
    this.enabled = enabled;
    // Clamp operator-supplied config to safe minimums. Floor the per-op timeout at 1000ms so a
    // too-small (or <=0) value can't make the timeout scheduler cancel ops before they can run.
    this.operationTimeoutMs = Math.max(1000L, operationTimeoutMs);

    // ThreadPoolExecutor throws if maximumPoolSize <= 0, and a 0-thread pool could never run
    // anything, so floor the pool size at 1 thread.
    int poolSize = Math.max(1, maxThreads);
    // Floor the queue at 100 so the pool has meaningful headroom to absorb bursts.
    int queueSize = Math.max(100, queueCapacity);
    // ThreadPoolExecutor forbids a zero keepAlive when core threads may time out.
    long keepAliveSeconds =
        reclaimIdleThreads
            ? Math.max(1L, idleThreadKeepAliveSeconds)
            : Math.max(0L, idleThreadKeepAliveSeconds);
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            keepAliveSeconds,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            daemonThreadFactory("post-commit-op"),
            new ThreadPoolExecutor.AbortPolicy());
    // When enabled, idle workers (down to zero) are reclaimed after keepAlive so an unused runner
    // holds no threads; new load lazily re-creates them on demand up to the pool size. When
    // disabled, the pool holds a warm set of threads to avoid create/destroy churn under bursty
    // traffic.
    tpe.allowCoreThreadTimeOut(reclaimIdleThreads);
    this.executor = tpe;
    this.timeoutScheduler =
        Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("post-commit-timeout"));

    log.info(
        "PostCommitOperationRunner initialized (enabled={}) with {} operation(s): {} (maxThreads={}, queueCapacity={}, idleThreadKeepAliveSeconds={}, reclaimIdleThreads={}, operationTimeoutMs={})",
        enabled,
        this.operations.size(),
        operationNames(),
        poolSize,
        queueSize,
        keepAliveSeconds,
        reclaimIdleThreads,
        this.operationTimeoutMs);
  }

  /** Whether post-commit dispatch is enabled; a server-side kill switch for the whole seam. */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Submits every registered operation for asynchronous, best-effort execution. Returns
   * immediately; never throws.
   *
   * @param context committed table context
   */
  public void runAll(PostCommitContext context) {
    if (!enabled || operations.isEmpty()) {
      return;
    }
    operations.forEach(operation -> submit(operation, context));
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
      // Queue full: drop rather than block the committer or grow memory unbounded. This is an
      // expected best-effort outcome under load and is also tracked by the "rejected" counter.
      count(name, "rejected");
      log.warn(
          "Post-commit operation '{}' rejected (pool saturated) for table {}",
          name,
          context.getTableIdentifier());
    } catch (Throwable submitFailure) {
      // Unexpected, but nonfatal: the commit has already durably succeeded and is unaffected.
      count(name, "rejected");
      log.warn("Failed to submit post-commit operation '{}' (nonfatal)", name, submitFailure);
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
