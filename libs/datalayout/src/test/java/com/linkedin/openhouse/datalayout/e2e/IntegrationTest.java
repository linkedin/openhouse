package com.linkedin.openhouse.datalayout.e2e;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.detection.DataCompactionTrigger;
import com.linkedin.openhouse.datalayout.detection.FileEntropyPolicy;
import com.linkedin.openhouse.datalayout.layoutselection.DataCompactionLayout;
import com.linkedin.openhouse.datalayout.layoutselection.OpenHouseLayoutSelectionPolicy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntegrationTest extends OpenHouseSparkITest {
  @Test
  public void testLayoutSelectionWithPersistence() throws Exception {
    final String testTable = "db.test_table_selection";
    try (SparkSession spark = withCatalogSession()) {
      createTestTable(spark, testTable, 10);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      OpenHouseLayoutSelectionPolicy layoutSelectionPolicy =
          OpenHouseLayoutSelectionPolicy.builder().tableFileStats(tableFileStats).build();
      DataCompactionLayout compactionLayout = layoutSelectionPolicy.evaluate();
      Assertions.assertEquals(
          DataCompactionLayout.TARGET_SIZE_BYTES_DEFAULT, compactionLayout.getTargetSizeBytes());
      Gson gson = new GsonBuilder().create();
      String serializedLayout = gson.toJson(compactionLayout);
      spark.sql(
          String.format(
              "alter table %s set tblproperties ('data-layout' = '%s')",
              testTable, StringEscapeUtils.escapeJava(serializedLayout)));
      serializedLayout =
          spark
              .sql(String.format("show tblproperties %s ('data-layout')", testTable))
              .collectAsList()
              .get(0)
              .getString(1);
      compactionLayout =
          gson.fromJson(
              StringEscapeUtils.unescapeJava(serializedLayout), DataCompactionLayout.class);
      Assertions.assertEquals(
          DataCompactionLayout.TARGET_SIZE_BYTES_DEFAULT, compactionLayout.getTargetSizeBytes());
    }
  }

  @Test
  public void testLayoutRegressionDetection() throws Exception {
    final String testTable = "db.test_table_detection";
    try (SparkSession spark = withCatalogSession()) {
      createTestTable(spark, testTable, 10);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      DataCompactionLayout compactionLayout =
          DataCompactionLayout.builder()
              .targetSizeBytes(DataCompactionLayout.TARGET_SIZE_BYTES_DEFAULT)
              .build();
      DataCompactionTrigger<FileStat, DataCompactionLayout, TableFileStats> trigger =
          FileEntropyPolicy.builder()
              .targetLayout(compactionLayout)
              .tableFileStats(tableFileStats)
              .threshold(100.0)
              .build();
      Assertions.assertTrue(trigger.check());
    }
  }

  private void createTestTable(SparkSession spark, String tableName, int numRows) {
    spark.sql(String.format("create table %s (id int, data string)", tableName));
    for (int i = 0; i < numRows; ++i) {
      spark.sql(String.format("insert into %s values (%d, 'data')", tableName, i));
    }
  }

  private SparkSession withCatalogSession() throws Exception {
    SparkSession session = getSparkSession();
    session.sql("use openhouse");
    return session;
  }
}
