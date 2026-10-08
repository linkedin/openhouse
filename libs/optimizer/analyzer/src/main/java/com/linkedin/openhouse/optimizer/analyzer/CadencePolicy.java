package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Time-based scheduling policy. An analyzer delegates to {@link CadencePolicy} to decide whether to
 * re-issue a recommendation for a table.
 *
 * <p>Two independent guards:
 *
 * <ol>
 *   <li><b>Active-op guard.</b> The analyzer stays out of any table that already has a <i>live</i>
 *       operation — those belong to the scheduler. A {@code CANCELED} row never blocks. A {@code
 *       PENDING} row (not yet claimed) always blocks. A {@code SCHEDULING}/{@code SCHEDULED} row
 *       blocks only until its <b>deadline</b>: once a submitted job fails to reach a terminal
 *       SUCCESS/FAILED within {@code activeOpStaleTimeout} — it died, hung, or its completion
 *       callback was missed — the row is treated as dead and no longer blocks, so the table is
 *       rescheduled rather than wedged forever. The timeout length encodes the bias: short for
 *       cheap/idempotent work (prefer an extra job), long for expensive/mutating work (prefer a
 *       missed job over a duplicate).
 *   <li><b>Cadence guard.</b> With no live op, the decision is based on the most recent completed
 *       history entry: re-evaluate after {@code successRetryInterval} on success, or after {@code
 *       failureRetryInterval} on failure (shorter, for quick recovery).
 *   <li><b>Circuit breaker.</b> Once a table accrues {@code failureStreakThreshold} consecutive
 *       FAILED runs, the failure interval grows exponentially per additional failure (capped at
 *       {@code failureBackoffMax}) so a chronically-failing table stops burning jobs. A single
 *       SUCCESS resets the streak, restoring the flat interval — automatic recovery.
 * </ol>
 *
 * <p>The deadline here is derived from the op's {@code scheduledAt} (falling back to {@code
 * createdAt}) plus {@code activeOpStaleTimeout}. A future enhancement can store an explicit
 * per-operation deadline at schedule time to support per-table overrides (e.g. a larger table gets
 * a longer deadline); this policy already treats the stale timeout as that default deadline.
 */
public class CadencePolicy {

  /** Default active-op stale timeout when a caller does not configure one. */
  static final Duration DEFAULT_ACTIVE_OP_STALE_TIMEOUT = Duration.ofHours(3);

  /** Default consecutive-failure count at which exponential backoff engages. */
  static final int DEFAULT_FAILURE_STREAK_THRESHOLD = 3;

  /** Default ceiling on the backed-off failure retry interval. */
  static final Duration DEFAULT_FAILURE_BACKOFF_MAX = Duration.ofHours(24);

  /**
   * How long to wait after a successful operation before re-evaluating the table. For example, if
   * set to 16 hours and OFD succeeded at 10:00 AM Monday, the table becomes eligible again at 2:00
   * AM Tuesday. Configured below 24h so that at least one re-evaluation is guaranteed within any
   * rolling 24-hour window regardless of when the prior run landed.
   */
  private final Duration successRetryInterval;

  /**
   * How long to wait after a failed operation before retrying. Shorter than the success interval to
   * allow quick recovery. For example, if set to 1 hour and OFD failed at 2:00 PM, the table
   * becomes eligible for retry at 3:00 PM.
   */
  private final Duration failureRetryInterval;

  /**
   * How long a {@code SCHEDULING}/{@code SCHEDULED} operation may sit without reaching a terminal
   * history entry before it is considered dead (missed/hung job) and stops blocking reschedule.
   * Measured from the op's {@code scheduledAt} (fallback {@code createdAt}).
   */
  private final Duration activeOpStaleTimeout;

  /**
   * Consecutive-FAILED-history count at which the circuit breaker engages exponential backoff.
   * Below this, a failed run retries at the flat {@link #failureRetryInterval} (transient failures
   * recover fast); at or above it, the retry interval doubles per additional failure.
   */
  private final int failureStreakThreshold;

  /** Ceiling on the backed-off failure retry interval, so backoff cannot grow without bound. */
  private final Duration failureBackoffMax;

  public CadencePolicy(
      Duration successRetryInterval,
      Duration failureRetryInterval,
      Duration activeOpStaleTimeout,
      int failureStreakThreshold,
      Duration failureBackoffMax) {
    this.successRetryInterval = successRetryInterval;
    this.failureRetryInterval = failureRetryInterval;
    this.activeOpStaleTimeout = activeOpStaleTimeout;
    this.failureStreakThreshold = failureStreakThreshold;
    this.failureBackoffMax = failureBackoffMax;
  }

  /**
   * Uses {@link #DEFAULT_FAILURE_STREAK_THRESHOLD} and {@link #DEFAULT_FAILURE_BACKOFF_MAX} for the
   * circuit breaker.
   */
  public CadencePolicy(
      Duration successRetryInterval, Duration failureRetryInterval, Duration activeOpStaleTimeout) {
    this(
        successRetryInterval,
        failureRetryInterval,
        activeOpStaleTimeout,
        DEFAULT_FAILURE_STREAK_THRESHOLD,
        DEFAULT_FAILURE_BACKOFF_MAX);
  }

  /** Uses {@link #DEFAULT_ACTIVE_OP_STALE_TIMEOUT} for the active-op deadline. */
  public CadencePolicy(Duration successRetryInterval, Duration failureRetryInterval) {
    this(successRetryInterval, failureRetryInterval, DEFAULT_ACTIVE_OP_STALE_TIMEOUT);
  }

  /** Whether a table's consecutive-failure streak has tripped the circuit breaker. */
  public boolean isBreakerTripped(int consecutiveFailures) {
    return consecutiveFailures >= failureStreakThreshold;
  }

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   * @param consecutiveFailures number of consecutive FAILED history entries ending at {@code
   *     latestHistory}; {@code 0} when the latest run did not fail. Drives exponential backoff once
   *     it reaches {@link #failureStreakThreshold}.
   */
  public boolean shouldSchedule(
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory,
      int consecutiveFailures) {
    if (currentOp.isPresent() && isBlockingActiveOp(currentOp.get())) {
      return false;
    }
    return latestHistory
        .map(entry -> readyAfterHistoryEntry(entry, consecutiveFailures))
        .orElse(true);
  }

  /**
   * Whether an existing operation row should keep the analyzer from issuing a new recommendation.
   * CANCELED never blocks; PENDING always blocks (waiting for the scheduler to claim it); a
   * SCHEDULING/SCHEDULED row blocks only until it passes {@link #activeOpStaleTimeout}, after which
   * it is treated as a dead job eligible for rescheduling.
   */
  private boolean isBlockingActiveOp(TableOperationDto op) {
    switch (op.getStatus()) {
      case CANCELED:
        return false;
      case PENDING:
        return true;
      case SCHEDULING:
      case SCHEDULED:
        return !isPastStaleTimeout(op);
      default:
        throw new IllegalStateException("Unhandled OperationStatusDto value: " + op.getStatus());
    }
  }

  private boolean isPastStaleTimeout(TableOperationDto op) {
    Instant reference = op.getScheduledAt() != null ? op.getScheduledAt() : op.getCreatedAt();
    if (reference == null) {
      // Cannot determine the op's age; conservatively treat it as live so we never reschedule a
      // job we can't prove is stale.
      return false;
    }
    return Duration.between(reference, Instant.now()).compareTo(activeOpStaleTimeout) > 0;
  }

  private boolean readyAfterHistoryEntry(TableOperationsHistoryDto entry, int consecutiveFailures) {
    return Duration.between(entry.getCompletedAt(), Instant.now())
            .compareTo(intervalFor(entry.getStatus(), consecutiveFailures))
        > 0;
  }

  private Duration intervalFor(HistoryStatusDto status, int consecutiveFailures) {
    // Explicit per-status mapping. Adding a new HistoryStatusDto value forces this switch to
    // grow a case; the default throws so an un-handled value surfaces at runtime rather than
    // silently falling into the success bucket.
    switch (status) {
      case SUCCESS:
        return successRetryInterval;
      case FAILED:
        return failureInterval(consecutiveFailures);
      default:
        throw new IllegalStateException("Unhandled HistoryStatusDto value: " + status);
    }
  }

  /**
   * Flat {@link #failureRetryInterval} until the streak reaches {@link #failureStreakThreshold};
   * from there the interval doubles per additional consecutive failure (exponential backoff),
   * capped at {@link #failureBackoffMax}. A single SUCCESS resets the streak to 0, restoring the
   * flat interval — this is the circuit breaker's automatic recovery.
   */
  private Duration failureInterval(int consecutiveFailures) {
    if (consecutiveFailures < failureStreakThreshold) {
      return failureRetryInterval;
    }
    int doublings = consecutiveFailures - 1;
    // Guard against overflow from shifting a large streak; cap the exponent well before 2^63.
    if (doublings >= 62) {
      return failureBackoffMax;
    }
    Duration backed = failureRetryInterval.multipliedBy(1L << doublings);
    return backed.compareTo(failureBackoffMax) > 0 ? failureBackoffMax : backed;
  }
}
