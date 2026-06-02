package com.linkedin.openhouse.optimizer.operations.ofd;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * OFD-specific {@link BinItem}: carries the table fqtn and operation id the downstream Spark
 * dispatch needs, plus the weight (current file count) the packer uses. Self-weights from a paired
 * {@link TableOperationDto} and {@link TableStatsDto} via {@link #from(TableOperationDto,
 * TableStatsDto)}.
 *
 * <p>Weighting choice — file count, not bytes — reflects what makes OFD expensive: per-file
 * listing, manifest joins, and delete calls scale with file count. A 10 GB table with 100k files is
 * more expensive to OFD than a 1 TB table with 2k files.
 */
@AllArgsConstructor
@Getter
@ToString
public class OfdBinItem implements BinItem {

  /** Fully-qualified {@code database.table} identifier passed as {@code --tableNames}. */
  @NonNull private final String fqtn;

  /**
   * Optimizer operation id passed as {@code --operationIds}; the Spark app POSTs back keyed on it.
   */
  @NonNull private final String operationId;

  /** Current file count for this table; the FFD packer's cost dimension. */
  private final long weight;

  /**
   * Project a pending operation + its stats row into a packable item. Weighting lives entirely in
   * this class — callers do {@code pendingOps.stream().map(op -> OfdBinItem.from(op,
   * statsByUuid.get(op.getTableUuid())))}.
   */
  public static OfdBinItem from(@NonNull TableOperationDto op, TableStatsDto stats) {
    return new OfdBinItem(
        op.getDatabaseName() + "." + op.getTableName(), op.getId(), currentFileCount(stats));
  }

  private static long currentFileCount(TableStatsDto stats) {
    return Optional.ofNullable(stats)
        .map(TableStatsDto::getSnapshot)
        .map(TableStatsDto.SnapshotMetrics::getNumCurrentFiles)
        .orElse(0L);
  }

  @Override
  public long getWeight() {
    return weight;
  }
}
