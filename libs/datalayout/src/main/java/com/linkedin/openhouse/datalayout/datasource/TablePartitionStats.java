package com.linkedin.openhouse.datalayout.datasource;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/** Data source implementation for table partition statistics. */
@Builder
public class TablePartitionStats implements DataSource<PartitionStat> {
  private final SparkSession spark;
  private final String tableName;

  @Override
  public Dataset<PartitionStat> get() {
    StructType partitionSchema =
        spark.sql(String.format("SELECT * FROM %s.partitions", tableName)).schema();
    try {
      partitionSchema.apply("partition");
      return spark
          .sql(String.format("SELECT partition, file_count FROM %s.partitions", tableName))
          .map(new TablePartitionStats.PartitionStatMapper(), Encoders.bean(PartitionStat.class));
    } catch (IllegalArgumentException e) {
      return spark
          .sql(String.format("SELECT null, file_count FROM %s.partitions", tableName))
          .map(new TablePartitionStats.PartitionStatMapper(), Encoders.bean(PartitionStat.class));
    }
  }

  static class PartitionStatMapper implements MapFunction<Row, PartitionStat> {
    @Override
    public PartitionStat call(Row row) {
      List<String> values = new ArrayList<>();
      Row partition = row.getStruct(0);
      if (partition != null) {
        for (int i = 0; i < partition.size(); i++) {
          values.add(partition.get(i).toString());
        }
      }
      return PartitionStat.builder().values(values).fileCount(row.getInt(1)).build();
    }
  }
}
