package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
  private static final long NUM_FILE_GROUPS_PER_COMMIT = 100;
  private static final long MAX_NUM_COMMITS = 30;
  private static final long MAX_BYTES_SIZE_RATIO = 10;
  private static final long REWRITE_BYTES_PER_SECOND = 2 * MB;
  private static final long EXECUTOR_MEMORY_GB = 2;
  private static final int MAX_CONCURRENT_FILE_GROUP_REWRITES = 50;
  private static final int REWRITE_PARALLELISM = 900; // number of Spark tasks to run in parallel
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
    return generateCompactionStrategy()
        .map(Collections::singletonList)
        .orElse(Collections.emptyList());
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
  private Optional<DataLayoutStrategy> generateCompactionStrategy() {
    // Retrieve file sizes of all data files.
    Dataset<Long> fileSizes =
        tableFileStats.get().map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG());

    Dataset<Long> filteredSizes =
        fileSizes.filter(
            (FilterFunction<Long>)
                size ->
                    size < TARGET_BYTES_SIZE * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT);
    // Check whether we have anything to map/reduce on for cost computation, this is only the case
    // if we have small files that need to be compacted.
    if (filteredSizes.count() == 0) {
      return Optional.empty();
    }

    // Traits computation (cost, gain, and entropy).
    Tuple2<Long, Integer> fileStats =
        filteredSizes
            .map(
                (MapFunction<Long, Tuple2<Long, Integer>>) size -> new Tuple2<>(size, 1),
                Encoders.tuple(Encoders.LONG(), Encoders.INT()))
            .reduce(
                (ReduceFunction<Tuple2<Long, Integer>>)
                    (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
    long rewriteFileBytes = fileStats._1;
    int rewriteFileCount = fileStats._2;

    DataCompactionConfig.DataCompactionConfigBuilder configBuilder = DataCompactionConfig.builder();

    configBuilder.targetByteSize(TARGET_BYTES_SIZE);

    long estimatedFileGroupsCount =
        Math.max(
            tablePartitionStats.get().count(),
            rewriteFileBytes / DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    int maxCommitsCount =
        (int)
            Math.min(
                MAX_NUM_COMMITS,
                (estimatedFileGroupsCount + NUM_FILE_GROUPS_PER_COMMIT - 1)
                    / NUM_FILE_GROUPS_PER_COMMIT);
    configBuilder.partialProgressMaxCommits(maxCommitsCount);

    int estimatedTasksPerFileGroupCount =
        (int) (DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT / TARGET_BYTES_SIZE);

    configBuilder.maxConcurrentFileGroupRewrites(
        Math.min(
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            (estimatedTasksPerFileGroupCount + REWRITE_PARALLELISM - 1)
                / estimatedTasksPerFileGroupCount));

    // don't split large files
    configBuilder.maxByteSizeRatio(MAX_BYTES_SIZE_RATIO);

    long reducedFileCount = estimateReducedFileCount(rewriteFileBytes, rewriteFileCount);
    double computeGbHr = estimateComputeGbHr(rewriteFileBytes);
    // computeGbHr >= COMPUTE_STARTUP_COST_GB_HR
    double reducedFileCountPerComputeGbHr = reducedFileCount / computeGbHr;
    return Optional.of(
        DataLayoutStrategy.builder()
            .config(configBuilder.build())
            .computationTimestamp(System.currentTimeMillis())
            .cost(computeGbHr)
            .gain(reducedFileCount)
            .score(reducedFileCountPerComputeGbHr)
            .entropy(computeEntropy(fileSizes))
            .build());
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

  /** Computes the file entropy as the difference of a set of file's target and actual file size. */
  private double computeEntropy(Dataset<Long> fileSizes) {
    // If no files available, MSE is 0.
    if (fileSizes.count() == 0) {
      return 0;
    }
    // Compute the mean-squared error.
    double mse = 0.0;
    Iterator<Long> itr = fileSizes.toLocalIterator();
    while (itr.hasNext()) {
      mse += Math.pow(TARGET_BYTES_SIZE - itr.next(), 2);
    }
    // Normalize.
    return mse / fileSizes.count();
  }
}
