package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.TableOperationRecord;
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

  private final Duration successRetryInterval;
  private final Duration failureRetryInterval;
  private final Duration scheduledTimeout;

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  public boolean shouldSchedule(
      Optional<TableOperationRecord> currentOp, Optional<TableOperationHistoryRow> latestHistory) {
    if (currentOp.isEmpty()) {
      return decideFromHistory(latestHistory);
    }
    TableOperationRecord op = currentOp.get();
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
