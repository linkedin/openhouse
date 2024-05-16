package com.linkedin.openhouse.datalayout.e2e;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.detection.DataCompactionTrigger;
import com.linkedin.openhouse.datalayout.detection.FileEntropyTrigger;
import com.linkedin.openhouse.datalayout.layoutselection.DataOptimizationLayout;
import com.linkedin.openhouse.datalayout.layoutselection.OpenHouseLayoutSelectionPolicy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
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
      List<DataOptimizationLayout> compactionLayouts = layoutSelectionPolicy.evaluate();
      Assertions.assertEquals(526385152, compactionLayouts.get(0).getConfig().getTargetByteSize());
      Gson gson = new GsonBuilder().create();
      Type type = new TypeToken<ArrayList<DataOptimizationLayout>>() {}.getType();
      String serializedLayout = gson.toJson(compactionLayouts, type);
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
      compactionLayouts = gson.fromJson(StringEscapeUtils.unescapeJava(serializedLayout), type);
      Assertions.assertEquals(526385152, compactionLayouts.get(0).getConfig().getTargetByteSize());
    }
  }

  @Test
  public void testLayoutRegressionDetection() throws Exception {
    final String testTable = "db.test_table_detection";
    try (SparkSession spark = withCatalogSession()) {
      createTestTable(spark, testTable, 10);
      TableFileStats tableFileStats =
          TableFileStats.builder().tableName(testTable).spark(spark).build();
      DataOptimizationLayout compactionLayout =
          DataOptimizationLayout.builder().config(DataCompactionConfig.builder().build()).build();
      DataCompactionTrigger<FileStat, DataOptimizationLayout, TableFileStats> trigger =
          FileEntropyTrigger.builder()
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
