package com.linkedin.openhouse.datalayout.datasource;

import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Builder
public class TableSnapshotStats implements DataSource<SnapshotStat> {
  private final SparkSession spark;
  private final String tableName;

  @Override
  public Dataset<SnapshotStat> get() {
    return spark
        .sql(
            String.format(
                "SELECT committed_at, snapshot_id, operation FROM %s.snapshots", tableName))
        .map(new TableSnapshotStats.SnapshotStatMapper(), Encoders.bean(SnapshotStat.class));
  }

  static class SnapshotStatMapper implements MapFunction<Row, SnapshotStat> {
    @Override
    public SnapshotStat call(Row row) {
      return SnapshotStat.builder()
          .committedAt(row.getTimestamp(0).getTime())
          .snapshotId(row.getLong(1))
          .operation(row.getString(2))
          .build();
    }
  }
}
