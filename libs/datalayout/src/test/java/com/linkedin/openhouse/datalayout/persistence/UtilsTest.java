package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.layoutselection.DataLayoutOptimizationStrategy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest extends OpenHouseSparkITest {
  @Test
  public void testStrategyPersistence() throws Exception {
    final String testTable = "db.test_table_strategy_persistence";
    try (SparkSession spark = withCatalogSession()) {
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      DataLayoutOptimizationStrategy strategy =
          DataLayoutOptimizationStrategy.builder()
              .config(DataCompactionConfig.builder().build())
              .build();
      List<DataLayoutOptimizationStrategy> strategyList = Arrays.asList(strategy, strategy);
      Utils.saveStrategies(spark, testTable, strategyList);
      Assertions.assertEquals(strategyList, Utils.loadStrategies(spark, testTable));
    }
  }

  private SparkSession withCatalogSession() throws Exception {
    SparkSession session = getSparkSession();
    session.sql("use openhouse");
    return session;
  }
}
