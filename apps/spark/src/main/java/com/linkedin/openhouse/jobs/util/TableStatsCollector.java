package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
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
  String fqtn;
  Table table;

  /** Collect table stats. */
  public IcebergTableStats collectTableStats() {
    IcebergTableStats stats = IcebergTableStats.builder().build();

    IcebergTableStats statsWithMetadataData =
        TableStatsCollectorUtil.populateTableMetadata(table, stats);
    IcebergTableStats statsWithReferenceFiles =
        TableStatsCollectorUtil.populateStatsOfAllReferencedFiles(
            fqtn, table, spark, statsWithMetadataData);
    IcebergTableStats statsWithCurrentSnapshot =
        TableStatsCollectorUtil.populateStatsForSnapshots(
            fqtn, table, spark, statsWithReferenceFiles);

    IcebergTableStats tableStats =
        TableStatsCollectorUtil.populateStorageStats(fqtn, table, fs, statsWithCurrentSnapshot);

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
  public java.util.List<com.linkedin.openhouse.common.stats.model.CommitEventTable>
      collectCommitEventTable() {
    return TableStatsCollectorUtil.populateCommitEventTable(fqtn, table, spark);
  }
}
