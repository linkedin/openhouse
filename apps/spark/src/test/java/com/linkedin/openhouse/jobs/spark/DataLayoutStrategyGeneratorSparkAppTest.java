package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataLayoutStrategyGeneratorSparkAppTest extends OpenHouseSparkITest {
  @Test
  public void testPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // create table
      String fqtn = "u_openhouse.test_table";
      spark.sql(
          "create table openhouse.u_openhouse.test_table (id int, name string) partitioned by (id)");
      spark.sql("insert into openhouse.u_openhouse.test_table values (1, 'a'), (2, 'b')");
      // run app
      Operations ops = Operations.withCatalog(spark, null);
      DataLayoutStrategyGeneratorSparkApp app =
          new DataLayoutStrategyGeneratorSparkApp(
              "test-job-id",
              null,
              fqtn,
              "u_openhouse.dlo_strategies",
              "u_openhouse.dlo_partition_strategies");
      app.runInner(ops);
      // test
      Assertions.assertEquals(1, spark.sql("select * from u_openhouse.dlo_strategies").count());
      Assertions.assertEquals(
          2, spark.sql("select * from u_openhouse.dlo_partition_strategies").count());
      Assertions.assertTrue(
          spark
              .sql("select * from u_openhouse.dlo_strategies")
              .select("isPartitioned")
              .first()
              .getBoolean(0));
      Assertions.assertNotNull(ops.getTable(fqtn).properties().get("write.data-layout.strategies"));
      Assertions.assertNotNull(
          ops.getTable(fqtn).properties().get("write.data-layout.partition-strategies"));
      // drop table
      spark.sql("drop table openhouse.u_openhouse.test_table");
      spark.sql("drop table openhouse.u_openhouse.dlo_strategies");
      spark.sql("drop table openhouse.u_openhouse.dlo_partition_strategies");
    }
  }

  @Test
  public void testUnPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // create table
      String fqtn = "u_openhouse.test_table";
      spark.sql("create table openhouse.u_openhouse.test_table (id int, name string)");
      spark.sql("insert into openhouse.u_openhouse.test_table values (1, 'a'), (2, 'b')");
      // run app
      Operations ops = Operations.withCatalog(spark, null);
      DataLayoutStrategyGeneratorSparkApp app =
          new DataLayoutStrategyGeneratorSparkApp(
              "test-job-id",
              null,
              fqtn,
              "u_openhouse.dlo_strategies",
              "u_openhouse.dlo_partition_strategies");
      app.runInner(ops);
      // test
      Assertions.assertEquals(1, spark.sql("select * from u_openhouse.dlo_strategies").count());
      Assertions.assertThrows(
          NoSuchTableException.class,
          () -> spark.sql("select * from u_openhouse.dlo_partition_strategies"));
      Assertions.assertFalse(
          spark
              .sql("select * from u_openhouse.dlo_strategies")
              .select("isPartitioned")
              .first()
              .getBoolean(0));
      Assertions.assertNotNull(ops.getTable(fqtn).properties().get("write.data-layout.strategies"));
      Assertions.assertNull(
          ops.getTable(fqtn).properties().get("write.data-layout.partition-strategies"));
      // drop table
      spark.sql("drop table openhouse.u_openhouse.test_table");
      spark.sql("drop table openhouse.u_openhouse.dlo_strategies");
      spark.sql("drop table openhouse.u_openhouse.dlo_partition_strategies");
    }
  }
}
