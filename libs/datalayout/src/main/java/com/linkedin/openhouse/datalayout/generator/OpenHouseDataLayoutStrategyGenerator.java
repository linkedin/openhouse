package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.util.DataCompactionConfigUtil;
import com.linkedin.openhouse.datalayout.util.TableFileStatsUtil;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import scala.Tuple2;

/**
 * Data layout optimization strategies generator for OpenHouse. Generates a list of strategies with
 * score.
 */
@Builder
public class OpenHouseDataLayoutStrategyGenerator implements DataLayoutStrategyGenerator {
  private static final long MB = 1024 * 1024;
  private static final long FILE_BLOCK_SIZE_BYTES = 256 * MB;
  private static final long FILE_BLOCK_MARGIN_BYTES = 10 * MB;
  private static final long REWRITE_BYTES_PER_SECOND = 2 * MB;
  private static final long EXECUTOR_MEMORY_GB = 2;
  private static final long TARGET_BYTES_SIZE = 2 * FILE_BLOCK_SIZE_BYTES - FILE_BLOCK_MARGIN_BYTES;
  private static final double COMPUTE_STARTUP_COST_GB_HR = 0.5;
  private final TableFileStats tableFileStats;
  private final TablePartitionStats tablePartitionStats;

  /**
   * Generate a list of data layout optimization strategies based on the table file stats and
   * historic query patterns.
   */
  @Override
  public List<DataLayoutStrategy> generate() {
    return Collections.singletonList(generateCompactionStrategy());
  }

  /**
   * Computes the compaction strategy for a partitioned table. Only files less than the minimum
   * threshold are considered. The rewrite job parameters are calculated, as well as the following:
   *
   * <ul>
   *   <li>Estimated cost is computed as the GB-hrs spent in performing the compaction
   *   <li>Estimated gain is computed as the reduction in file count
   *   <li>Estimated score is computed as gain / cost, the number of files reduced per GB-hr of
   *       compute, the higher the score, the better the strategy
   * </ul>
   */
  private DataLayoutStrategy generateCompactionStrategy() {
    Tuple2<Long, Integer> aggFileStats =
        TableFileStatsUtil.getAggregatedFileStats(tableFileStats, TARGET_BYTES_SIZE);
    long rewriteFileBytes = aggFileStats._1;
    int rewriteFileCount = aggFileStats._2;

    long reducedFileCount = estimateReducedFileCount(rewriteFileBytes, rewriteFileCount);
    double computeGbHr = estimateComputeGbHr(rewriteFileBytes);
    // computeGbHr >= COMPUTE_STARTUP_COST_GB_HR
    double reducedFileCountPerComputeGbHr = reducedFileCount / computeGbHr;
    return DataLayoutStrategy.builder()
        .config(
            DataCompactionConfigUtil.createDefaultCompactionConfig(
                TARGET_BYTES_SIZE, tablePartitionStats.get().count(), rewriteFileBytes))
        .cost(computeGbHr)
        .gain(reducedFileCount)
        .score(reducedFileCountPerComputeGbHr)
        .build();
  }

  private long estimateReducedFileCount(long rewriteFileBytes, int rewriteFileCount) {
    // number of files after compaction rounded up
    long resultFileCount = (rewriteFileBytes + TARGET_BYTES_SIZE - 1) / TARGET_BYTES_SIZE;
    return Math.max(0, rewriteFileCount - resultFileCount);
  }

  private double estimateComputeGbHr(long rewriteBytes) {
    double rewriteSeconds = rewriteBytes * 1.0 / REWRITE_BYTES_PER_SECOND;
    double rewriteHours = rewriteSeconds / 3600.0;
    return rewriteHours * EXECUTOR_MEMORY_GB + COMPUTE_STARTUP_COST_GB_HR;
  }
}
