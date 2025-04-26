package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StrategiesDaoTablePropsTest extends OpenHouseSparkITest {
  @Test
  public void testStrategyPersistence() throws Exception {
    final String testTable = "db.test_table_strategy_persistence";
    try (SparkSession spark = withCatalogSession()) {
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      DataLayoutStrategy strategy =
          DataLayoutStrategy.builder().config(DataCompactionConfig.builder().build()).build();
      // validate up-to 100 strategies can be saved and loaded
      List<DataLayoutStrategy> strategyList = Collections.nCopies(2, strategy);
      StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
      dao.save(testTable, strategyList);
      dao.savePartitionScope(testTable, strategyList);
      Assertions.assertEquals(strategyList, dao.load(testTable));
      Assertions.assertEquals(strategyList, dao.loadPartitionScope(testTable));
      // validate delete
      dao.delete(testTable);
      dao.deletePartitionScope(testTable);
      Assertions.assertEquals(
          0,
          spark
              .sql(String.format("SHOW TBLPROPERTIES %s", testTable))
              .filter(
                  String.format(
                      "key='%s'", StrategiesDaoTableProps.DATA_LAYOUT_STRATEGIES_PROPERTY_KEY))
              .collectAsList()
              .size());
      Assertions.assertEquals(
          0,
          spark
              .sql(String.format("SHOW TBLPROPERTIES %s", testTable))
              .filter(
                  String.format(
                      "key='%s'",
                      StrategiesDaoTableProps.DATA_LAYOUT_STRATEGIES_PARTITION_PROPERTY_KEY))
              .collectAsList()
              .size());
    }
  }

  private SparkSession withCatalogSession() throws Exception {
    SparkSession session = getSparkSession();
    session.sql("use openhouse");
    return session;
  }
}
