package com.linkedin.openhouse.datalayout.datasource;

import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Data source implementation for table file statistics. */
@Builder
public class TableFileStats implements DataSource<FileStat> {
  private final SparkSession spark;
  private final String tableName;

  @Override
  public Dataset<FileStat> get() {
    return spark
        .sql(String.format("SELECT file_path, file_size_in_bytes FROM %s.files", tableName))
        .map(new FileStatMapper(), Encoders.bean(FileStat.class));
  }

  static class FileStatMapper implements MapFunction<Row, FileStat> {
    @Override
    public FileStat call(Row row) {
      return FileStat.builder().path(row.getString(0)).size(row.getLong(1)).build();
    }
  }
}
