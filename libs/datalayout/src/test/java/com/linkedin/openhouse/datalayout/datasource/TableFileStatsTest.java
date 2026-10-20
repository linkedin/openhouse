package com.linkedin.openhouse.datalayout.datasource;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableFileStatsTest extends OpenHouseSparkITest {
  @Test
  public void testNonPartitionedTableFileStats() throws Exception {
    final String testTable = "db_non_partitioned.test_table_file_stats";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (0, '')", testTable));
      spark.sql(
          String.format("INSERT INTO %s VALUES (100000000, '000000000000000000000')", testTable));
      TableFileStats tableFileStats =
          TableFileStats.builder().spark(spark).tableName(testTable).build();
      Map<String, Long> stats =
          tableFileStats.get().collectAsList().stream()
              .collect(Collectors.toMap(FileStat::getPath, FileStat::getSizeInBytes));
      FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
      Path tableDirectory =
          new Path(
                  spark
                      .sql(
                          String.format(
                              "SHOW TBLPROPERTIES %s ('openhouse.tableLocation')", testTable))
                      .collectAsList()
                      .get(0)
                      .getString(1))
              .getParent();
      Map<String, Long> expectedStats = new HashMap<>();
      for (FileStatus fileStatus : fs.listStatus(new Path(tableDirectory, "data"))) {
        expectedStats.put(
            fileStatus.getPath().toString().substring("file:".length()), fileStatus.getLen());
      }
      Assertions.assertEquals(expectedStats, stats);
    }
  }

  @Test
  public void testPartitionedTableFileStats() throws Exception {
    final String testTable = "db_partitioned.test_table_file_stats";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(
          String.format(
              "CREATE TABLE %s (ts TIMESTAMP, id INT, data STRING) PARTITIONED BY (days(ts), id)",
              testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (current_timestamp(), 0, '0')", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (current_timestamp(), 1, '1')", testTable));
      TableFileStats tableFileStats =
          TableFileStats.builder().spark(spark).tableName(testTable).build();
      List<FileStat> fileStatList = tableFileStats.get().collectAsList();
      FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
      Path tableDirectory =
          new Path(
                  spark
                      .sql(
                          String.format(
                              "SHOW TBLPROPERTIES %s ('openhouse.tableLocation')", testTable))
                      .collectAsList()
                      .get(0)
                      .getString(1))
              .getParent();
      for (FileStat fileStat : fileStatList) {
        String tsPartitionValue = fileStat.getPartitionValues().get(0);
        String idPartitionValue = fileStat.getPartitionValues().get(1);
        String folder = "data" + "/ts_day=" + tsPartitionValue + "/id=" + idPartitionValue;
        FileStatus fileStatus = fs.listStatus(new Path(tableDirectory, folder))[0];
        Assertions.assertEquals(fileStatus.getLen(), fileStat.getSizeInBytes());
      }
    }
  }
}
