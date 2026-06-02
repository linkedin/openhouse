package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;

/**
 * {@link BinItem} that weights by the table's current file count. Suitable for any operation whose
 * Spark cost scales with file count — orphan files deletion, stats collection, etc. The
 * implementation knows nothing about which operation type it is wired up to.
 *
 * <p>Construction: callers pass {@code TotalFilesBinItem::new} as the {@code Supplier<T>} to {@link
 * FirstFitBinPacker}; the packer calls the supplier per operation to get an empty instance, then
 * {@link #fromOpAndStats} on it to get a populated copy.
 */
@Getter
@ToString
public class TotalFilesBinItem implements BinItem {

  private final String fullyQualifiedTableName;
  private final String operationId;
  private final long weight;

  /** Empty constructor: call {@link #fromOpAndStats} on the result to get a populated instance. */
  public TotalFilesBinItem() {
    this("", "", 0L);
  }

  private TotalFilesBinItem(String fullyQualifiedTableName, String operationId, long weight) {
    this.fullyQualifiedTableName = fullyQualifiedTableName;
    this.operationId = operationId;
    this.weight = weight;
  }

  @Override
  public BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats) {
    return new TotalFilesBinItem(
        op.getDatabaseName() + "." + op.getTableName(), op.getId(), currentFileCount(stats));
  }

  private static long currentFileCount(TableStatsDto stats) {
    return Optional.ofNullable(stats)
        .map(TableStatsDto::getSnapshot)
        .map(TableStatsDto.SnapshotMetrics::getNumCurrentFiles)
        .orElse(0L);
  }
}
