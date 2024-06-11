package com.linkedin.openhouse.datalayout.datasource;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class ColumnStats implements DataSource<ColumnStat> {
  @Override
  public abstract Dataset<ColumnStat> get();

  static class ColumnStatMapper implements MapFunction<Row, ColumnStat> {
    @Override
    public ColumnStat call(Row row) {
      return ColumnStat.builder().name(row.getString(0)).accessCount(row.getLong(1)).build();
    }
  }
}
