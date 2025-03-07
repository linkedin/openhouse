package com.linkedin.openhouse.datalayout.e2e;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.generator.OpenHouseDataLayoutStrategyGenerator;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDao;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoInternal;
import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class IntegrationTest extends OpenHouseSparkITest {
  @Test
  public void testCompactionStrategyGenerationWithPersistencePartitioned() throws Exception {
    final String testTable = "db.test_table_partitioned";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      createTestTable(spark, testTable, 10, true);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().tableName(testTable).spark(spark).build();
      OpenHouseDataLayoutStrategyGenerator strategyGenerator =
          OpenHouseDataLayoutStrategyGenerator.builder()
              .tableFileStats(tableFileStats)
              .tablePartitionStats(tablePartitionStats)
              .build();
      List<DataLayoutStrategy> strategies = strategyGenerator.generatePartitionLevelStrategies();
      Assertions.assertEquals(10, strategies.size());

      StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
      dao.save(testTable, strategies);
      List<DataLayoutStrategy> retrievedStrategies = dao.load(testTable);
      Assertions.assertEquals(strategies, retrievedStrategies);

      StrategiesDao internalDao =
          StrategiesDaoInternal.builder()
              .spark(spark)
              .outputFqtn("db.dlo_output")
              .isPartitionScope(true)
              .build();
      internalDao.save(testTable, strategies);
      List<DataLayoutStrategy> retrievedStrategiesInternal = internalDao.load("db.dlo_output");
      for (int i = 0; i < strategies.size(); i++) {
        Assertions.assertEquals(
            strategies.get(i).getPosDeleteFileCount(),
            retrievedStrategiesInternal.get(i).getPosDeleteFileCount());
      }
    }
  }

  @Test
  public void testCompactionStrategyGenerationNonPartitioned() throws Exception {
    final String testTable = "db.test_table";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      createTestTable(spark, testTable, 10, false);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().tableName(testTable).spark(spark).build();
      OpenHouseDataLayoutStrategyGenerator strategyGenerator =
          OpenHouseDataLayoutStrategyGenerator.builder()
              .tableFileStats(tableFileStats)
              .tablePartitionStats(tablePartitionStats)
              .build();
      List<DataLayoutStrategy> strategies = strategyGenerator.generateTableLevelStrategies();
      Assertions.assertEquals(1, strategies.size());
      StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
      dao.save(testTable, strategies);
      List<DataLayoutStrategy> retrievedStrategies = dao.load(testTable);
      Assertions.assertEquals(strategies, retrievedStrategies);

      StrategiesDao internalDao =
          StrategiesDaoInternal.builder()
              .spark(spark)
              .outputFqtn("db.dlo_output2")
              .isPartitionScope(false)
              .build();
      internalDao.save(testTable, strategies);
      List<DataLayoutStrategy> retrievedStrategiesInternal = internalDao.load("db.dlo_output2");
      for (int i = 0; i < strategies.size(); i++) {
        Assertions.assertEquals(
            strategies.get(i).getPosDeleteFileCount(),
            retrievedStrategiesInternal.get(i).getPosDeleteFileCount());
      }
    }
  }

  private void createTestTable(
      SparkSession spark, String tableName, int numRows, boolean isPartitioned) {
    if (isPartitioned) {
      spark.sql(
          String.format(
              "create table %s (id int, data string, ts timestamp) partitioned by (days(ts))",
              tableName));
      for (int i = 0; i < numRows; ++i) {
        spark.sql(
            String.format(
                "insert into %s values (%d, 'data', date_sub(current_timestamp(), %d))",
                tableName, i, i));
      }
    } else {
      spark.sql(String.format("create table %s (id int, data string)", tableName));
      for (int i = 0; i < numRows; ++i) {
        spark.sql(String.format("insert into %s values (%d, 'data')", tableName, i));
      }
    }
  }
}
