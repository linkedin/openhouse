package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.Optional;

/**
 * First-fit-decreasing packing keyed on the table's current file count. Suitable for any operation
 * whose Spark cost scales with file count — orphan files deletion, stats collection, etc. The
 * packer knows nothing about which operation type it was registered against; the choice of weight
 * (current file count) is the only operation-shape assumption it encodes.
 */
public class TotalFilesFirstFitBinPacker extends FirstFitBinPacker<TotalFilesBinItem> {

  public TotalFilesFirstFitBinPacker(long maxWeightPerBin, int maxItemsPerBin) {
    super(maxWeightPerBin, maxItemsPerBin);
  }

  @Override
  protected TotalFilesBinItem create(TableOperationDto operation, TableStatsDto stats) {
    return new TotalFilesBinItem(
        operation.getDatabaseName() + "." + operation.getTableName(),
        operation.getId(),
        currentFileCount(stats));
  }

  private static long currentFileCount(TableStatsDto stats) {
    return Optional.ofNullable(stats)
        .map(TableStatsDto::getSnapshot)
        .map(TableStatsDto.SnapshotMetrics::getNumCurrentFiles)
        .orElse(0L);
  }
}
