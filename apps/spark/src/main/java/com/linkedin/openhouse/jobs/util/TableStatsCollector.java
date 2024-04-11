package com.linkedin.openhouse.jobs.util;

import com.google.gson.Gson;
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

  /** Collect and publish table stats. */
  public IcebergTableStats collectAndPublishTableStats() {
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

    publishStats(tableStats);
    return tableStats;
  }

  /**
   * Publish table stats.
   *
   * @param stats Table stats to publish
   */
  void publishStats(IcebergTableStats stats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(new Gson().toJson(stats));
  }
}
