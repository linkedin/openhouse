package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OrphanFilesDeletionSparkAppTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testTtlSecondsPrimaryTableOneDayTtl() throws Exception {
    final String tableName = "db.test_ofd1";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // create table
      prepareTable(ops, tableName);
      // create and test ofd spark job
      OrphanFilesDeletionSparkApp app =
          new OrphanFilesDeletionSparkApp(
              "test-ofd-job1", null, tableName, 86400, otelEmitter, ".backup");
      Assertions.assertEquals(86400, app.getTtlSeconds());
    }
  }

  @Test
  public void testTtlSecondsPrimaryTableThreeDayTtl() throws Exception {
    final String tableName = "db.test_ofd2";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // create table
      prepareTable(ops, tableName);
      // create and test ofd spark job
      OrphanFilesDeletionSparkApp app =
          new OrphanFilesDeletionSparkApp(
              "test-ofd-job2", null, tableName, 259200, otelEmitter, ".backup");
      Assertions.assertEquals(259200, app.getTtlSeconds());
    }
  }

  @Test
  public void testTtlSecondsReplicaTableOneDayTtl() throws Exception {
    final String tableName = "db.test_ofd3";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // create table
      prepareReplicaTable(ops, tableName);
      // create and test ofd spark job
      OrphanFilesDeletionSparkApp app =
          new OrphanFilesDeletionSparkApp(
              "test-ofd-job3", null, tableName, 86400, otelEmitter, ".backup");
      app.runInner(ops);
      Assertions.assertEquals(259200, app.getTtlSeconds());
    }
  }

  @Test
  public void testTtlSecondsReplicaTableThreeDayTtl() throws Exception {
    final String tableName = "db.test_ofd4";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // create table
      prepareReplicaTable(ops, tableName);
      // create and test ofd spark job
      OrphanFilesDeletionSparkApp app =
          new OrphanFilesDeletionSparkApp(
              "test-ofd-job4", null, tableName, 259200, otelEmitter, ".backup");
      app.runInner(ops);
      Assertions.assertEquals(259200, app.getTtlSeconds());
    }
  }

  private static void prepareTable(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) partitioned by (days(ts))", tableName))
        .show();
    ops.spark().sql(String.format("SHOW TBLPROPERTIES %s", tableName)).show(false);
  }

  private static void prepareReplicaTable(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) partitioned by (days(ts)) "
                    + "TBLPROPERTIES ('openhouse.tableType' = 'REPLICA_TABLE')",
                tableName))
        .show();
    ops.spark().sql(String.format("SHOW TBLPROPERTIES %s", tableName)).show(false);
  }
}
