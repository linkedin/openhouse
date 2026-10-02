package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
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
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  public boolean shouldSchedule(
      Optional<TableOperationDto> currentOp, Optional<TableOperationsHistoryDto> latestHistory) {
    if (currentOp.isPresent() && currentOp.get().getStatus() != OperationStatusDto.CANCELED) {
      return false;
    }
    return latestHistory.map(this::readyAfterHistoryEntry).orElse(true);
  }

  private boolean readyAfterHistoryEntry(TableOperationsHistoryDto entry) {
    return Duration.between(entry.getCompletedAt(), Instant.now())
            .compareTo(intervalFor(entry.getStatus()))
        > 0;
  }

  private Duration intervalFor(HistoryStatusDto status) {
    // Explicit per-status mapping. Adding a new HistoryStatusDto value forces this switch to
    // grow a case; the default throws so an un-handled value surfaces at runtime rather than
    // silently falling into the success bucket.
    switch (status) {
      case SUCCESS:
        return successRetryInterval;
      case FAILED:
        return failureRetryInterval;
      default:
        throw new IllegalStateException("Unhandled HistoryStatusDto value: " + status);
    }
  }
}
