package com.linkedin.openhouse.optimizer.scheduler.binpack;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * A single packable unit for a {@link BinPacker}. Carries enough identity for downstream consumers
 * (the optimizer scheduler dispatching Spark, the existing JobsScheduler, an offline analyzer) to
 * resolve the underlying table and report results without re-reading optimizer state.
 *
 * <p>{@link #weight} is the primary bin-packing dimension (for orphan files deletion: the number of
 * current files in the table). {@link #sizeBytes} is a secondary capacity dimension so a packer can
 * cap the on-disk footprint of a bin independently of file count.
 *
 * <p>This type is structurally identical to {@code jobs.util.binpack.BinItem} introduced by
 * PR&nbsp;#599. When that PR merges, this class becomes a redundant copy and we should switch the
 * scheduler to import the common one.
 */
@Getter
@Builder
@ToString
public class BinItem {
  /** Fully-qualified {@code database.table} identifier the batched Spark app will load. */
  @NonNull private final String fqtn;

  /** Optimizer operation id; the Spark app POSTs its outcome back keyed on this. */
  @NonNull private final String operationId;

  /** Stable table identity for stats lookup and history correlation. */
  @NonNull private final String tableUuid;

  @NonNull private final String databaseName;
  @NonNull private final String tableName;

  /** Primary packing cost — for OFD this is the table's current file count. */
  private final long weight;

  /** Secondary packing cost — on-disk size in bytes. {@code 0} when unknown. */
  private final long sizeBytes;
}
