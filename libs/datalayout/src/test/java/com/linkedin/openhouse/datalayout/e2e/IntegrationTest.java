package com.linkedin.openhouse.datalayout.e2e;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.layoutselection.DataLayoutOptimizationStrategy;
import com.linkedin.openhouse.datalayout.layoutselection.OpenHouseDataLayoutGenerator;
import com.linkedin.openhouse.datalayout.persistence.Utils;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntegrationTest extends OpenHouseSparkITest {
  @Test
  public void testCompactionStrategyGenerationWithPersistence() throws Exception {
    final String testTable = "db.test_table";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      createTestTable(spark, testTable, 10);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      OpenHouseDataLayoutGenerator strategyGenerator =
          OpenHouseDataLayoutGenerator.builder().tableFileStats(tableFileStats).build();
      List<DataLayoutOptimizationStrategy> strategies = strategyGenerator.generate();
      Assertions.assertEquals(1, strategies.size());
      Assertions.assertEquals(1, strategies.get(0).getConfig().getPartialProgressMaxCommits());
      Assertions.assertTrue(strategies.get(0).getConfig().isPartialProgressEnabled());
      Utils.saveStrategies(spark, testTable, strategies);
      List<DataLayoutOptimizationStrategy> retrievedStrategies =
          Utils.loadStrategies(spark, testTable);
      Assertions.assertEquals(strategies, retrievedStrategies);
    }
  }

  private void createTestTable(SparkSession spark, String tableName, int numRows) {
    spark.sql(String.format("create table %s (id int, data string)", tableName));
    for (int i = 0; i < numRows; ++i) {
      spark.sql(String.format("insert into %s values (%d, 'data')", tableName, i));
    }
  }
}
