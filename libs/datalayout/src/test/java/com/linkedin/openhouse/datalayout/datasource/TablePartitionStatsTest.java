package com.linkedin.openhouse.datalayout.datasource;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TablePartitionStatsTest extends OpenHouseSparkITest {
  @Test
  public void testPartitionedTablePartitionStats() throws Exception {
    final String testTable = "db.test_table_partition_stats_partitioned";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format(
              "CREATE TABLE %s (id INT, data STRING, dt STRING) PARTITIONED BY (dt, id)",
              testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (0, '0', '2024-01-01')", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (1, '1', '2024-01-02')", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (1, '2', '2024-01-02')", testTable));
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().spark(spark).tableName(testTable).build();
      List<PartitionStat> stats = tablePartitionStats.get().collectAsList();
      Assertions.assertEquals(2, stats.size());
      stats.sort(Comparator.comparing(a -> a.getValues().get(0)));
      Assertions.assertEquals(Arrays.asList("2024-01-01", "0"), stats.get(0).getValues());
      Assertions.assertEquals(1, stats.get(0).getFileCount());
      Assertions.assertEquals(Arrays.asList("2024-01-02", "1"), stats.get(1).getValues());
      Assertions.assertEquals(2, stats.get(1).getFileCount());
    }
  }

  @Test
  public void testPartitionedTableNulls() throws Exception {
    final String testTable = "db.test_table_partition_stats_partitioned_null";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format("CREATE TABLE %s (id INT, data STRING) PARTITIONED BY (id)", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (null, '0')", testTable));
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().spark(spark).tableName(testTable).build();
      List<PartitionStat> stats = tablePartitionStats.get().collectAsList();
      Assertions.assertEquals(1, stats.size());
    }
  }

  @Test
  public void testNonPartitionedTablePartitionStats() throws Exception {
    final String testTable = "db.test_table_partition_stats_non_partitioned";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (0, '0')", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (1, '1')", testTable));
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().spark(spark).tableName(testTable).build();
      List<PartitionStat> stats = tablePartitionStats.get().collectAsList();
      Assertions.assertEquals(1, stats.size());
      Assertions.assertTrue(stats.get(0).getValues().isEmpty());
      Assertions.assertEquals(2, stats.get(0).getFileCount());
    }
  }

  @Test
  public void testGetPartitionColumnsPartitioned() throws Exception {
    final String testTable = "db.test_table_partition_cols_partitioned";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format(
              "CREATE TABLE %s (ts TIMESTAMP, id INT, data STRING) PARTITIONED BY (days(ts), id)",
              testTable));
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().spark(spark).tableName(testTable).build();
      List<String> partitionColumns = tablePartitionStats.getPartitionColumns();
      List<String> expectedPartitionColumns = Arrays.asList("ts_day", "id");
      Assertions.assertEquals(expectedPartitionColumns, partitionColumns);
    }
  }

  @Test
  public void testGetPartitionColumnsNonPartitioned() throws Exception {
    final String testTable = "db.test_table_partition_cols_non_partitioned";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      TablePartitionStats tablePartitionStats =
          TablePartitionStats.builder().spark(spark).tableName(testTable).build();
      Assertions.assertEquals(Collections.emptyList(), tablePartitionStats.getPartitionColumns());
    }
  }
}
