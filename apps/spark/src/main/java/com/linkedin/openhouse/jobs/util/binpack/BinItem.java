package com.linkedin.openhouse.jobs.util.binpack;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * A single packable unit for {@link FirstFitDecreasingBinPacker}. Carries everything the batched
 * Spark app needs both to do the work ({@link #fqtn}) and to report the result back to the
 * Optimizer Service ({@link #operationId}, {@link #tableUuid}, {@link #databaseName}, {@link
 * #tableName}).
 *
 * <p>{@link #weight} is the bin-packing dimension (for OFD: number of current files in the table).
 * {@link #sizeBytes} is a secondary capacity dimension that lets the packer cap the total on-disk
 * footprint of a bin independently of file count.
 */
@Getter
@Builder
@ToString
public class BinItem {
  @NonNull private final String fqtn;
  @NonNull private final String operationId;
  @NonNull private final String tableUuid;
  @NonNull private final String databaseName;
  @NonNull private final String tableName;
  private final long weight;
  private final long sizeBytes;
}
