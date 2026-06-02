package com.linkedin.openhouse.optimizer.scheduler.binpack;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * {@link BinItem} that weights by the table's current file count. Immutable data; constructed by
 * the {@link TotalFilesFirstFitBinPacker}. The implementation knows nothing about which operation
 * type the surrounding packer was registered against — it just carries the fields the scheduler
 * needs to launch the job.
 */
@AllArgsConstructor
@Getter
@ToString
public class TotalFilesBinItem implements BinItem {
  private final String fullyQualifiedTableName;
  private final String operationId;
  private final long weight;
}
