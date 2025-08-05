package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataLayoutStrategyGeneratorSparkAppTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // create table
      String fqtn = "dlo_partitioned.test_table";
      String outputFqtn = "dlo_partitioned.dlo_strategies";
      String partitionLevelOutputFqtn = "dlo_partitioned.dlo_partition_strategies";
      spark.sql(
          String.format(
              "create table openhouse.%s (id int, name string) partitioned by (id)", fqtn));
      spark.sql(String.format("insert into openhouse.%s values (1, 'a'), (2, 'b')", fqtn));
      // run app
      Operations ops = Operations.withCatalog(spark, null);
      DataLayoutStrategyGeneratorSparkApp app =
          new DataLayoutStrategyGeneratorSparkApp(
              "test-job-id", null, fqtn, outputFqtn, partitionLevelOutputFqtn, otelEmitter);
      app.runInner(ops);
      // test
      Assertions.assertEquals(1, spark.sql(String.format("select * from %s", outputFqtn)).count());
      Assertions.assertEquals(
          2, spark.sql(String.format("select * from %s", partitionLevelOutputFqtn)).count());
      Assertions.assertTrue(
          spark
              .sql(String.format("select * from %s", outputFqtn))
              .select("isPartitioned")
              .first()
              .getBoolean(0));
      Assertions.assertNotNull(ops.getTable(fqtn).properties().get("write.data-layout.strategies"));
      Assertions.assertNull(
          ops.getTable(fqtn).properties().get("write.data-layout.partition-strategies"));
    }
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // create table
      String fqtn = "dlo_unpartitioned.test_table";
      String outputFqtn = "dlo_unpartitioned.dlo_strategies";
      String partitionLevelOutputFqtn = "dlo_unpartitioned.dlo_partition_strategies";
      spark.sql(String.format("create table openhouse.%s (id int, name string)", fqtn));
      spark.sql(String.format("insert into openhouse.%s values (1, 'a'), (2, 'b')", fqtn));
      // run app
      Operations ops = Operations.withCatalog(spark, null);
      DataLayoutStrategyGeneratorSparkApp app =
          new DataLayoutStrategyGeneratorSparkApp(
              "test-job-id", null, fqtn, outputFqtn, partitionLevelOutputFqtn, otelEmitter);
      app.runInner(ops);
      // test
      Assertions.assertEquals(1, spark.sql(String.format("select * from %s", outputFqtn)).count());
      Assertions.assertThrows(
          AnalysisException.class,
          () -> spark.sql(String.format("select * from %s", partitionLevelOutputFqtn)));
      Assertions.assertFalse(
          spark
              .sql(String.format("select * from %s", outputFqtn))
              .select("isPartitioned")
              .first()
              .getBoolean(0));
      Assertions.assertNotNull(ops.getTable(fqtn).properties().get("write.data-layout.strategies"));
      Assertions.assertNull(
          ops.getTable(fqtn).properties().get("write.data-layout.partition-strategies"));
    }
  }
}
