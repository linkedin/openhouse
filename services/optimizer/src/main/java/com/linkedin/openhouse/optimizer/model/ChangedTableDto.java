package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.db.HistoryStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;

/**
 * Projection over the analyzer's incremental join: a changed table plus its current operation (if
 * any) and its latest completed-history entry (if any). Built from the {@code [TableStatsRow,
 * TableOperationsRow, <lastCompletedAt>, <lastStatus>]} tuple returned by {@code
 * TableStatsRepository.findChangedWithOpAndLatestHistory}.
 */
@Value
@Builder
public class ChangedTableDto {

  /** The changed table. Never null. */
  TableDto table;

  /** The table's current active operation for the analyzed type, or null if none. */
  TableOperationDto currentOp;

  /** The table's most recent completed-history entry for the analyzed type, or null if none. */
  TableOperationsHistoryDto latestHistory;

  public Optional<TableOperationDto> currentOp() {
    return Optional.ofNullable(currentOp);
  }

  public Optional<TableOperationsHistoryDto> latestHistory() {
    return Optional.ofNullable(latestHistory);
  }

  /**
   * Map a join tuple to a {@link ChangedTableDto}. Expected layout: {@code [0]=TableStatsRow,
   * [1]=TableOperationsRow (nullable), [2]=latest completed_at (nullable), [3]=latest status
   * (nullable)}. The latest history is populated only when both the timestamp and status are
   * present.
   */
  public static ChangedTableDto fromJoinRow(Object[] row) {
    TableStatsRow statsRow = (TableStatsRow) row[0];
    TableOperationsRow opRow = (TableOperationsRow) row[1];
    Instant lastCompletedAt = toInstant(row[2]);
    HistoryStatus lastStatus = toHistoryStatus(row[3]);

    TableOperationsHistoryDto latest = null;
    if (lastCompletedAt != null && lastStatus != null) {
      latest =
          TableOperationsHistoryDto.builder()
              .tableUuid(statsRow.getTableUuid())
              .databaseName(statsRow.getDatabaseName())
              .tableName(statsRow.getTableName())
              .completedAt(lastCompletedAt)
              .status(HistoryStatusDto.fromDb(lastStatus))
              .build();
    }
    return ChangedTableDto.builder()
        .table(TableDto.fromRow(statsRow))
        .currentOp(opRow == null ? null : TableOperationDto.fromRow(opRow))
        .latestHistory(latest)
        .build();
  }

  /** Tolerate whichever temporal type the JPA provider returns for {@code MAX(completedAt)}. */
  private static Instant toInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    if (value instanceof Date) {
      return ((Date) value).toInstant();
    }
    throw new IllegalStateException(
        "Unexpected completedAt type from join: " + value.getClass().getName());
  }

  private static HistoryStatus toHistoryStatus(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof HistoryStatus) {
      return (HistoryStatus) value;
    }
    if (value instanceof String) {
      return HistoryStatus.valueOf((String) value);
    }
    throw new IllegalStateException(
        "Unexpected history status type from join: " + value.getClass().getName());
  }
}
