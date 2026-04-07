package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/**
 * Encapsulates the time-based scheduling logic shared across operation types. An analyzer delegates
 * to {@link CadencePolicy} to decide whether to re-issue a recommendation for a table that already
 * has an active operation record and/or history.
 *
 * <p>The SCHEDULED timeout is a key safety mechanism: if a Spark job crashes without reporting
 * back, the SCHEDULED row would otherwise block the table forever. When the row has been SCHEDULED
 * (or SCHEDULING) longer than {@code scheduledTimeout}, the Analyzer treats it as stale and returns
 * {@code true}, causing a new PENDING row to be inserted.
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
   * Maximum time a row can stay in SCHEDULED status before the analyzer treats it as stale and
   * overwrites it with a new PENDING row. Handles the case where a Spark job crashes without
   * reporting back. For example, if set to 6 hours and a job was submitted at noon but never
   * completed, the analyzer will re-schedule the table after 6:00 PM.
   */
  private final Duration scheduledTimeout;

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  public boolean shouldSchedule(
      Optional<TableOperation> currentOp, Optional<TableOperationHistoryRow> latestHistory) {
    if (currentOp.isEmpty()) {
      return decideFromHistory(latestHistory);
    }
    TableOperation op = currentOp.get();
    switch (op.getStatus()) {
      case "PENDING":
      case "SCHEDULING":
        return false;
      case "SCHEDULED":
        if (latestHistory.isEmpty()) {
          return pastInterval(op.getScheduledAt(), scheduledTimeout);
        }
        return decideFromHistoryEntry(latestHistory.get());
      default:
        return true;
    }
  }

  private boolean decideFromHistory(Optional<TableOperationHistoryRow> latestHistory) {
    if (latestHistory.isEmpty()) {
      return true;
    }
    return decideFromHistoryEntry(latestHistory.get());
  }

  private boolean decideFromHistoryEntry(TableOperationHistoryRow entry) {
    switch (entry.getStatus()) {
      case "SUCCESS":
        return pastInterval(entry.getSubmittedAt(), successRetryInterval);
      case "FAILED":
        return pastInterval(entry.getSubmittedAt(), failureRetryInterval);
      default:
        return true;
    }
  }

  private boolean pastInterval(Instant timestamp, Duration interval) {
    return timestamp == null || Duration.between(timestamp, Instant.now()).compareTo(interval) > 0;
  }
}
