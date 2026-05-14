package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/**
 * Time-based scheduling policy. An analyzer delegates to {@link CadencePolicy} to decide whether to
 * re-issue a recommendation for a table.
 *
 * <p>The analyzer stays out of any table that already has a non-CANCELED active operation — those
 * belong to the scheduler. For tables with no active operation (or only a CANCELED one), the
 * decision is based on the most recent completed-history entry: re-evaluate after {@code
 * successRetryInterval} on success, or after {@code failureRetryInterval} on failure.
 */
@RequiredArgsConstructor
public class CadencePolicy {

  /**
   * How long to wait after a successful operation before re-evaluating the table. For example, if
   * set to 24 hours and OFD succeeded at 10:00 AM Monday, the table won't be scheduled again until
   * after 10:00 AM Tuesday.
   */
  private final Duration successRetryInterval;

  /**
   * How long to wait after a failed operation before retrying. Shorter than the success interval to
   * allow quick recovery. For example, if set to 1 hour and OFD failed at 2:00 PM, the table
   * becomes eligible for retry at 3:00 PM.
   */
  private final Duration failureRetryInterval;

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  public boolean shouldSchedule(
      Optional<TableOperation> currentOp, Optional<TableOperationHistoryRow> latestHistory) {
    if (currentOp.isPresent() && currentOp.get().getStatus() != OperationStatus.CANCELED) {
      return false;
    }
    return latestHistory.map(this::readyAfterHistoryEntry).orElse(true);
  }

  private boolean readyAfterHistoryEntry(TableOperationHistoryRow entry) {
    HistoryStatus status = HistoryStatus.valueOf(entry.getStatus());
    Duration interval =
        status == HistoryStatus.FAILED ? failureRetryInterval : successRetryInterval;
    return Duration.between(entry.getCompletedAt(), Instant.now()).compareTo(interval) > 0;
  }
}
