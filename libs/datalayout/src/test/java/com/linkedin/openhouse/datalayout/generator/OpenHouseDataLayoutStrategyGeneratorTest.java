package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OpenHouseDataLayoutStrategyGeneratorTest extends OpenHouseSparkITest {
  @Test
  void testStrategySanityCheck() throws Exception {
    final String testTable = "db.test_table_sanity_check";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format(
              "create table %s (id int, data string, ts timestamp) partitioned by (days(ts))",
              testTable));

      // produce 2 partitions
      for (int i = 0; i < 3; ++i) {
        spark.sql(
            String.format(
                "insert into %s values (0, 'data', cast('2024-07-15 00:1%d:34' as timestamp))",
                testTable, i));
      }
      for (int i = 0; i < 3; ++i) {
        spark.sql(
            String.format(
                "insert into %s values (0, 'data', cast('2024-07-16 00:1%d:34' as timestamp))",
                testTable, i));
      }

      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().tableName(testTable).spark(spark).build();
      OpenHouseDataLayoutStrategyGenerator strategyGenerator =
          OpenHouseDataLayoutStrategyGenerator.builder()
              .tableFileStats(tableFileStats)
              .tablePartitionStats(tablePartitionStats)
              .build();
      List<DataLayoutStrategy> strategies = strategyGenerator.generate();
      Assertions.assertEquals(1, strategies.size());
      DataLayoutStrategy strategy = strategies.get(0);
      // few groups, expect 1 commit
      Assertions.assertEquals(1, strategy.getConfig().getPartialProgressMaxCommits());
      Assertions.assertTrue(strategy.getConfig().isPartialProgressEnabled());

      Assertions.assertTrue(
          strategy.getGain() == 5, "Gain for 6 files compaction in 2 partitions should be 5");
      Assertions.assertTrue(
          strategy.getCost() < 1.0, "Cost for 6 files compaction should be negligible");
      Assertions.assertTrue(
          strategy.getScore() < 10.0, "Score for 6 files compaction should be negligible");
    }
  }

  @Test
  void testPartitionLevelStrategySanityCheck() throws Exception {
    final String testTable = "db_partition.test_table_sanity_check";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format(
              "create table %s (id int, data string, ts timestamp) partitioned by (days(ts), data)",
              testTable));

      // produce 2 partitions
      for (int i = 0; i < 3; ++i) {
        spark.sql(
            String.format(
                "insert into %s values (%d, 'data1', cast('2025-02-15 00:1%d:34' as timestamp))",
                testTable, i, i));
      }
      for (int i = 0; i < 3; ++i) {
        spark.sql(
            String.format(
                "insert into %s values (%d, 'data2', cast('2025-02-16 00:1%d:34' as timestamp))",
                testTable, i, i));
      }

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
      Assertions.assertEquals(2, strategies.size());
    }
  }
}
