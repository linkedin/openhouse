package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

/**
 * Data layout optimization strategies generator for OpenHouse. Generates a list of strategies with
 * cost.
 */
@Builder
public class OpenHouseDataLayoutGenerator implements DataLayoutGenerator {
  private static final long FILE_BLOCK_SIZE_BYTES = 1024L * 1024L * 256; // 256MB
  private static final long FILE_BLOCK_MARGIN_BYTES = 1024L * 1024L * 10; // 10MB
  private static final long NUM_FILE_GROUPS_PER_COMMIT = 50;
  private static final double FILE_ENTROPY_THRESHOLD = 100.0;
  private static final long FILE_OPEN_COST_IN_BYTES = 1024L * 1024L * 4; // 4MB
  private static final long REWRITE_BYTES_PER_SECOND = 1024L * 1024L * 2; // 2MB/s
  private static final int REWRITE_PARALLELISM = 900; // number of Spark tasks to run in parallel
  private final TableFileStats tableFileStats;

  /**
   * Generate a list of data layout optimization strategies based on the table file stats and
   * historic query patterns.
   */
  @Override
  public List<DataLayoutOptimizationStrategy> generate() {
    return Collections.singletonList(generateCompactionStrategy());
  }

  private DataLayoutOptimizationStrategy generateCompactionStrategy() {
    long totalSizeBytes =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .reduce((ReduceFunction<Long>) Long::sum);

    DataCompactionConfig.DataCompactionConfigBuilder configBuilder = DataCompactionConfig.builder();

    long targetBytesSize = calculateTargetBytesSize();
    configBuilder.targetByteSize(targetBytesSize);

    // TODO: take partitioning into account
    // This is a under-estimation if partitions are smaller than
    // DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT
    int estimatedMaxNumFileGroups =
        Math.max(
            1, (int) (totalSizeBytes / DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT));

    int maxNumCommits =
        (int)
            Math.min(
                1000L,
                (estimatedMaxNumFileGroups + NUM_FILE_GROUPS_PER_COMMIT - 1)
                    / NUM_FILE_GROUPS_PER_COMMIT);
    configBuilder.partialProgressMaxCommits(maxNumCommits);

    int estimatedNumTasksPerFileGroup =
        (int) (DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT / targetBytesSize);

    // TODO: take partitioning into account
    // This is a under-estimation if partitions are much smaller than
    // DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT
    configBuilder.maxConcurrentFileGroupRewrites(
        Math.min(
            50,
            (estimatedNumTasksPerFileGroup + REWRITE_PARALLELISM - 1)
                / estimatedNumTasksPerFileGroup));

    // don't split large files
    configBuilder.maxByteSizeRatio(10);

    return DataLayoutOptimizationStrategy.builder()
        .config(configBuilder.build())
        .cost(calculateCompactionCost())
        .gain(calculateCompactionGain())
        .build();
  }

  private long calculateTargetBytesSize() {
    // Make sure the last block is almost full
    return 2 * FILE_BLOCK_SIZE_BYTES - FILE_BLOCK_MARGIN_BYTES;
  }

  /** Calculate the gain of compaction in terms of number of files reduced. */
  private double calculateCompactionGain() {
    long targetBytesSize = calculateTargetBytesSize();
    Dataset<Long> compactionCandidates =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .filter(
                (FilterFunction<Long>)
                    size ->
                        size < targetBytesSize * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT);
    long numFiles = compactionCandidates.count();
    long totalSizeBytes = compactionCandidates.reduce((ReduceFunction<Long>) Long::sum);
    return (double) (numFiles - totalSizeBytes / targetBytesSize);
  }

  /** Calculate the cost of compaction in seconds. */
  private double calculateCompactionCost() {
    return (double)
            tableFileStats
                .get()
                .map(
                    (MapFunction<FileStat, Long>)
                        (fs -> Math.max(fs.getSize(), FILE_OPEN_COST_IN_BYTES)),
                    Encoders.LONG())
                .reduce((ReduceFunction<Long>) Long::sum)
        / REWRITE_BYTES_PER_SECOND;
  }
}
