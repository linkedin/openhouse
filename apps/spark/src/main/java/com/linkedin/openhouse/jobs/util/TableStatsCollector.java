package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.stats.model.CommitEventTable;
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
}
