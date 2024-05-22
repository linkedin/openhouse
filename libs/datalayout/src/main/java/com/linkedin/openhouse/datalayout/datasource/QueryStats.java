package com.linkedin.openhouse.datalayout.datasource;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class QueryStats implements DataSource<QueryStat> {
  @Override
  public abstract Dataset<QueryStats> get();

  static class QueryStatMapper implements MapFunction<Row, QueryStat> {
    @Override
    public QueryStat call(Row row) {
      // TODO: implement
      return null;
    }
  }
}
