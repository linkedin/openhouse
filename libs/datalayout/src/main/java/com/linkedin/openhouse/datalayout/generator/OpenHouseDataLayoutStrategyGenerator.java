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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.Builder;
import org.apache.iceberg.FileContent;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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

    Dataset<FileStat> dataFiles =
        fileStats.filter((FilterFunction<FileStat>) file -> file.getContent() == FileContent.DATA);

    Dataset<FileStat> filteredDataFiles =
        dataFiles.filter(
            (FilterFunction<FileStat>)
                file ->
                    file.getSizeInBytes()
                        < TARGET_BYTES_SIZE * DataCompactionConfig.MIN_BYTE_SIZE_RATIO_DEFAULT);

    // Check whether we have anything to map/reduce on for cost computation, this is only the case
    // if we have small files that need to be compacted.
    if (filteredDataFiles.count() == 0) {
      return Optional.empty();
    }

    Tuple3<Long, Integer, Long> filteredDataFileStats =
        computeFileStats(filteredDataFiles, FileContent.DATA);
    Tuple3<Long, Integer, Long> posDeleteStats =
        computeFileStats(fileStats, FileContent.POSITION_DELETES);
    Tuple3<Long, Integer, Long> eqDeleteStats =
        computeFileStats(fileStats, FileContent.EQUALITY_DELETES);

    long rewriteFileBytes = filteredDataFileStats._1();
    int rewriteFileCount = filteredDataFileStats._2();

    long reducedFileCount = estimateReducedFileCount(rewriteFileBytes, rewriteFileCount);
    double computeGbHr = estimateComputeGbHr(rewriteFileBytes);
    // computeGbHr >= COMPUTE_STARTUP_COST_GB_HR
    double reducedFileCountPerComputeGbHr = reducedFileCount / computeGbHr;

    // don't discount for partitioned tables
    double fileCountReductionDiscount = 0.0;
    if (partitionValues == null) {
      fileCountReductionDiscount =
          computeFileCountReductionDiscount(tableSnapshotStats.get(), LAST_SNAPSHOT_LOOKBACK_DAYS);
    }

    DataCompactionConfig config = buildDataCompactionConfig(rewriteFileBytes, partitionCount);
    return Optional.of(
        DataLayoutStrategy.builder()
            .config(config)
            .cost(computeGbHr)
            .gain(reducedFileCount)
            .score(reducedFileCountPerComputeGbHr)
            .entropy(
                computeEntropy(
                    dataFiles.map(
                        (MapFunction<FileStat, Long>) FileStat::getSizeInBytes, Encoders.LONG())))
            .partitionId(partitionValues)
            .partitionColumns(partitionColumns)
            .posDeleteFileBytes(posDeleteStats._1())
            .eqDeleteFileBytes(eqDeleteStats._1())
            .posDeleteFileCount(posDeleteStats._2())
            .eqDeleteFileCount(eqDeleteStats._2())
            .posDeleteRecordCount(posDeleteStats._3())
            .eqDeleteRecordCount(eqDeleteStats._3())
            .fileCountReductionDiscount(fileCountReductionDiscount)
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

  private double computeFileCountReductionDiscount(
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

  /**
   * Computes statistics for files of a content type, including total bytes, file count, and record
   * count.
   *
   * @param files Dataset of FileStat objects
   * @param content a type of file in iceberg
   * @return A Tuple3 containing (totalBytes, fileCount, recordCount)
   */
  private Tuple3<Long, Integer, Long> computeFileStats(
      Dataset<FileStat> files, FileContent content) {
    Stream<FileStat> filesOfContent =
        files.collectAsList().stream().filter(file -> file.getContent().equals(content));

    return filesOfContent
        .map(file -> new Tuple3<>(file.getSizeInBytes(), 1, file.getRecordCount()))
        .reduce(
            new Tuple3<>(0L, 0, 0L), // Default value for empty collection
            (a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3() + b._3()));
  }
}
