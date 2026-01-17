package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;

/** Class to collect and publish stats for a given table. */
@Slf4j
@AllArgsConstructor
public class TableStatsCollector {

  private FileSystem fs;
  private SparkSession spark;
  Table table;

  /** Collect table stats. */
  public IcebergTableStats collectTableStats() {
    IcebergTableStats stats = IcebergTableStats.builder().build();

    IcebergTableStats statsWithMetadataData =
        TableStatsCollectorUtil.populateTableMetadata(table, stats);
    IcebergTableStats statsWithReferenceFiles =
        TableStatsCollectorUtil.populateStatsOfAllReferencedFiles(
            table, spark, statsWithMetadataData);
    IcebergTableStats statsWithCurrentSnapshot =
        TableStatsCollectorUtil.populateStatsForSnapshots(table, spark, statsWithReferenceFiles);

    IcebergTableStats tableStats =
        TableStatsCollectorUtil.populateStorageStats(table, fs, statsWithCurrentSnapshot);

    return tableStats;
  }

  /**
   * Collect commit events for the table.
   *
   * <p>Note: Returns List (loads into memory). This is acceptable because Iceberg retention limits
   * active snapshots to a manageable number (typically <10k per table).
   *
   * @return List of CommitEventTable objects (event_timestamp_ms will be set at publish time)
   */
  public List<CommitEventTable> collectCommitEventTable() {
    return TableStatsCollectorUtil.populateCommitEventTable(table, spark);
  }

  /**
   * Collect partition-level commit events for the table.
   *
   * <p>Returns one record per (commit_id, partition) pair. Returns empty list for unpartitioned
   * tables.
   *
   * <p>Note: Returns List (loads into memory). Size is manageable due to:
   *
   * <ul>
   *   <li>Iceberg retention limits active snapshots (~1-10k per table)
   *   <li>Typical partitions per commit: 10-1000
   *   <li>Typical size: 100K rows × 200 bytes = 20MB
   * </ul>
   *
   * @return List of CommitEventTablePartitions objects (event_timestamp_ms will be set at publish
   *     time)
   */
  public List<CommitEventTablePartitions> collectCommitEventTablePartitions() {
    return TableStatsCollectorUtil.populateCommitEventTablePartitions(table, spark);
  }

  /**
   * Collect statistics for the table (partitioned or unpartitioned).
   *
   * <p><b>For PARTITIONED tables:</b> Returns one record per unique partition with aggregated
   * statistics from data_files metadata table. Each partition is associated with its LATEST commit
   * (highest committed_at timestamp).
   *
   * <p><b>For UNPARTITIONED tables:</b> Returns a single record with aggregated statistics from ALL
   * data_files and current snapshot metadata. This ensures unpartitioned tables also report stats
   * with latest commit info at every job run.
   *
   * <p><b>Key differences from collectCommitEventTablePartitions:</b>
   *
   * <ul>
   *   <li><b>Granularity:</b> One record per unique partition (not per commit-partition pair), or
   *       single record for unpartitioned
   *   <li><b>Commit Association:</b> Latest commit only (max committed_at or current snapshot)
   *   <li><b>Data Source:</b> Includes statistics aggregated from data_files metadata table
   *   <li><b>Metrics:</b> Contains row count, column count, and field-level statistics
   * </ul>
   *
   * <p>Note: Returns List (loads into memory). Size is typically smaller than partition events
   * since we deduplicate to one record per unique partition (or single record for unpartitioned).
   *
   * @return List of CommitEventTablePartitionStats objects (event_timestamp_ms will be set at
   *     publish time)
   */
  public List<CommitEventTablePartitionStats> collectCommitEventTablePartitionStats() {
    return TableStatsCollectorUtil.populateCommitEventTablePartitionStats(table, spark);
  }
}
