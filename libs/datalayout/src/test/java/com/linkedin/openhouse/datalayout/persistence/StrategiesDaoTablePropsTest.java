package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.RewriteStrategy;
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
      RewriteStrategy strategy =
          RewriteStrategy.builder().config(DataCompactionConfig.builder().build()).build();
      // validate up-to 100 strategies can be saved and loaded
      List<RewriteStrategy> strategyList = Collections.nCopies(100, strategy);
      StrategiesDao dao = StrategiesDaoTableProps.builder().spark(spark).build();
      dao.save(testTable, strategyList);
      Assertions.assertEquals(strategyList, dao.load(testTable));
    }
  }

  private SparkSession withCatalogSession() throws Exception {
    SparkSession session = getSparkSession();
    session.sql("use openhouse");
    return session;
  }
}
