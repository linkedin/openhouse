package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutOptimizationStrategy;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;

/**
 * Data layout optimization strategies generator for OpenHouse. Generates a list of strategies with
 * cost.
 */
@Builder
public class OpenHouseDataLayoutGenerator implements DataLayoutGenerator {
  private static final long MB = 1024 * 1024;
  private static final long FILE_BLOCK_SIZE_BYTES = 256 * MB;
  private static final long FILE_BLOCK_MARGIN_BYTES = 10 * MB;
  private static final long NUM_FILE_GROUPS_PER_COMMIT = 100;
  private static final long MAX_NUM_COMMITS = 30;
  private static final long MAX_BYTES_SIZE_RATIO = 10;
  private static final long REWRITE_BYTES_PER_SECOND = 2 * MB;
  private static final long EXECUTOR_MEMORY_MB = 2048;
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
  public List<DataLayoutOptimizationStrategy> generate() {
    // skip single partition and non-partitioned tables
    if (tablePartitionStats.get().count() <= 1) {
      return Collections.emptyList();
    }
    return Collections.singletonList(generateCompactionStrategy());
  }

  private DataLayoutOptimizationStrategy generateCompactionStrategy() {
    List<Long> fileSizes =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .filter(
                (FilterFunction<Long>)
                    size ->
                        size < TARGET_BYTES_SIZE * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT)
            .collectAsList();

    DataCompactionConfig.DataCompactionConfigBuilder configBuilder = DataCompactionConfig.builder();

    configBuilder.targetByteSize(TARGET_BYTES_SIZE);

    long totalSizeBytes = fileSizes.stream().reduce(0L, Long::sum);
    long estimatedFileGroupsCount =
        Math.max(
            tablePartitionStats.get().count(),
            totalSizeBytes / DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

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

    long filesReducedCount = estimateReducedFilesCount(fileSizes);
    double computeGbHr = estimateComputeGbHr(fileSizes);
    double filesReducedCountPerComputeGbHr = filesReducedCount / computeGbHr;
    return DataLayoutOptimizationStrategy.builder()
        .config(configBuilder.build())
        .cost(computeGbHr)
        .gain(filesReducedCount)
        .score(filesReducedCountPerComputeGbHr)
        .build();
  }

  private long estimateReducedFilesCount(List<Long> fileSizes) {
    long candidateFilesBytes = fileSizes.stream().reduce(0L, Long::sum);
    long compactedFilesCount = (candidateFilesBytes + TARGET_BYTES_SIZE - 1) / TARGET_BYTES_SIZE;
    return Math.max(0, fileSizes.size() - compactedFilesCount);
  }

  private double estimateComputeGbHr(List<Long> fileSizes) {
    double candidateFilesBytes = fileSizes.stream().reduce(0L, Long::sum);
    double rewriteSeconds = candidateFilesBytes / REWRITE_BYTES_PER_SECOND;
    double rewriteHours = rewriteSeconds / 3600.0;
    return rewriteHours * EXECUTOR_MEMORY_MB / 1024 + COMPUTE_STARTUP_COST_GB_HR;
  }
}
