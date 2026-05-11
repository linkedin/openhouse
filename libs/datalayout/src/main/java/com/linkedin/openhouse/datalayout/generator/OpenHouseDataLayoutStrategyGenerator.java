package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.PartitionStat;
import com.linkedin.openhouse.datalayout.datasource.SnapshotStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.datasource.TableSnapshotStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.iceberg.FileContent;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import scala.Tuple3;

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
  private static final int LAST_SNAPSHOT_LOOKBACK_DAYS = 7;
  private final TableFileStats tableFileStats;
  private final TablePartitionStats tablePartitionStats;
  private final TableSnapshotStats tableSnapshotStats;
  private final boolean partitioned;

  /**
   * Generate a list of data layout optimization strategies based on the table file stats and
   * historic query patterns. For now we only use table-level strategies. TODO: add logic to
   * determine whether to generate table-level or partition-level strategies.
   */
  @Override
  public List<DataLayoutStrategy> generate() {
    return generateTableLevelStrategies();
  }

  /**
   * Generate a list of data layout optimization strategies based on the table file stats on the
   * table level.
   */
  @Override
  public List<DataLayoutStrategy> generateTableLevelStrategies() {
    // Retrieve file sizes of all data files.
    Dataset<FileStat> fileStats = tableFileStats.get();
    long partitionCount = tablePartitionStats.get().count();
    Optional<DataLayoutStrategy> strategy =
        buildDataLayoutStrategy(fileStats, partitionCount, null, null);
    return strategy.map(Collections::singletonList).orElse(Collections.emptyList());
  }

  /**
   * Generate a list of data layout optimization strategies based on the table file stats on the
   * partition level.
   */
  @Override
  public List<DataLayoutStrategy> generatePartitionLevelStrategies() {
    List<DataLayoutStrategy> strategies = new ArrayList<>();
    List<PartitionStat> partitionStatsList = tablePartitionStats.get().collectAsList();
    String partitionColumns = String.join(", ", tablePartitionStats.getPartitionColumns());
    // For each partition, generate a compaction strategy
    partitionStatsList.forEach(
        partitionStat -> {
          String partitionValues = String.join(", ", partitionStat.getValues());
          Dataset<FileStat> fileStats =
              tableFileStats
                  .get()
                  .filter(
                      (FilterFunction<FileStat>)
                          fileStat ->
                              String.join(", ", fileStat.getPartitionValues())
                                  .equals(partitionValues));
          Optional<DataLayoutStrategy> strategy =
              buildDataLayoutStrategy(fileStats, 1L, partitionValues, partitionColumns);
          strategy.ifPresent(strategies::add);
        });
    return strategies;
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
  private Optional<DataLayoutStrategy> buildDataLayoutStrategy(
      Dataset<FileStat> fileStats,
      long partitionCount,
      String partitionValues,
      String partitionColumns) {

    // Single Spark action: collect all files once, then partition/aggregate in driver memory.
    // Replaces 4 separate Spark actions (count + 3x collectAsList) with 1.
    List<FileStat> allFiles = fileStats.collectAsList();

    long minByteSize =
        (long) (TARGET_BYTES_SIZE * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT);

    Tuple3<Long, Integer, Long> filteredDataFileStats =
        aggregateInMemory(
            allFiles, f -> f.getContent() == FileContent.DATA && f.getSizeInBytes() < minByteSize);

    // Early-return if no small data files to compact.
    if (filteredDataFileStats._2() == 0) {
      return Optional.empty();
    }

    Tuple3<Long, Integer, Long> posDeleteStats =
        aggregateInMemory(allFiles, f -> f.getContent() == FileContent.POSITION_DELETES);
    Tuple3<Long, Integer, Long> eqDeleteStats =
        aggregateInMemory(allFiles, f -> f.getContent() == FileContent.EQUALITY_DELETES);

    // Derive DATA file sizes (used by computeEntropy) from the same in-memory list instead of
    // issuing another Spark action.
    List<Long> dataFileSizes =
        allFiles.stream()
            .filter(f -> f.getContent() == FileContent.DATA)
            .map(FileStat::getSizeInBytes)
            .collect(Collectors.toList());

    long rewriteFileBytes = filteredDataFileStats._1();
    int rewriteFileCount = filteredDataFileStats._2();

    long reducedFileCount = estimateReducedFileCount(rewriteFileBytes, rewriteFileCount);
    double computeGbHr = estimateComputeGbHr(rewriteFileBytes);
    // computeGbHr >= COMPUTE_STARTUP_COST_GB_HR
    double reducedFileCountPerComputeGbHr = reducedFileCount / computeGbHr;

    // don't discount for partitioned tables
    double fileCountReductionPenalty = 0.0;
    if (!partitioned) {
      fileCountReductionPenalty =
          computeFileCountReductionPenalty(tableSnapshotStats.get(), LAST_SNAPSHOT_LOOKBACK_DAYS);
    }

    DataCompactionConfig config = buildDataCompactionConfig(rewriteFileBytes, partitionCount);
    return Optional.of(
        DataLayoutStrategy.builder()
            .config(config)
            .cost(computeGbHr)
            .gain(reducedFileCount)
            .score(reducedFileCountPerComputeGbHr)
            .entropy(computeEntropy(dataFileSizes))
            .partitionId(partitionValues)
            .partitionColumns(partitionColumns)
            .posDeleteFileBytes(posDeleteStats._1())
            .eqDeleteFileBytes(eqDeleteStats._1())
            .posDeleteFileCount(posDeleteStats._2())
            .eqDeleteFileCount(eqDeleteStats._2())
            .posDeleteRecordCount(posDeleteStats._3())
            .eqDeleteRecordCount(eqDeleteStats._3())
            .fileCountReductionPenalty(fileCountReductionPenalty)
            .build());
  }

  private DataCompactionConfig buildDataCompactionConfig(
      long rewriteFileBytes, long partitionCount) {
    long estimatedFileGroupsCount =
        Math.max(
            partitionCount,
            rewriteFileBytes / DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    int maxCommitsCount =
        (int)
            Math.min(
                MAX_NUM_COMMITS,
                (estimatedFileGroupsCount + NUM_FILE_GROUPS_PER_COMMIT - 1)
                    / NUM_FILE_GROUPS_PER_COMMIT);

    int estimatedTasksPerFileGroupCount =
        (int) (DataCompactionConfig.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT / TARGET_BYTES_SIZE);

    int maxConcurrentFileGroupRewrites =
        Math.min(
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            (estimatedTasksPerFileGroupCount + REWRITE_PARALLELISM - 1)
                / estimatedTasksPerFileGroupCount);

    return DataCompactionConfig.builder()
        .targetByteSize(TARGET_BYTES_SIZE)
        .partialProgressMaxCommits(maxCommitsCount)
        .maxConcurrentFileGroupRewrites(maxConcurrentFileGroupRewrites)
        .maxByteSizeRatio(MAX_BYTES_SIZE_RATIO) // don't split large files
        .build();
  }

  private double computeFileCountReductionPenalty(
      Dataset<SnapshotStat> snapshotStats, int lookbackDays) {
    long snapshotCount =
        snapshotStats
            .filter(
                (FilterFunction<SnapshotStat>)
                    snapshotStat ->
                        snapshotStat.getCommittedAt()
                            >= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(lookbackDays))
            .count();
    // if there are new snapshots committed in the past N days,
    // then assume file count reduction is 0,
    // because the table could have been overwritten
    return snapshotCount == 0 ? 0.0 : 1.0;
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

  /**
   * Computes file entropy as mean-squared error from TARGET_BYTES_SIZE over a pre-collected list.
   */
  private double computeEntropy(List<Long> fileSizes) {
    if (fileSizes.isEmpty()) {
      return 0;
    }
    double mse = 0.0;
    for (long size : fileSizes) {
      double diff = TARGET_BYTES_SIZE - (double) size;
      mse += diff * diff;
    }
    return mse / fileSizes.size();
  }

  /**
   * Aggregates a (totalBytes, count, totalRecords) tuple from a pre-collected list of FileStats
   * matching the given predicate. Pure driver-side; issues no Spark actions.
   */
  private Tuple3<Long, Integer, Long> aggregateInMemory(
      List<FileStat> files, Predicate<FileStat> predicate) {
    long totalBytes = 0L;
    int count = 0;
    long totalRecords = 0L;
    for (FileStat f : files) {
      if (predicate.test(f)) {
        totalBytes += f.getSizeInBytes();
        count++;
        totalRecords += f.getRecordCount();
      }
    }
    return new Tuple3<>(totalBytes, count, totalRecords);
  }
}
