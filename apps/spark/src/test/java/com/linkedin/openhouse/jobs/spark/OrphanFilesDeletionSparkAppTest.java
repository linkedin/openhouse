package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class OrphanFilesDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final String BACKUP_DIR = ".backup";
  private static final String ORPHAN_FILE_NAME = "data/test_orphan_file.orc";
  private static final long DEFAULT_TTL_SECONDS = TimeUnit.DAYS.toSeconds(7);
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
              "test-ofd-job1", null, tableName, 86400, otelEmitter, ".backup", 1, false, 100);
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
              "test-ofd-job2", null, tableName, 259200, otelEmitter, ".backup", 1, false, 100);
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
              "test-ofd-job3", null, tableName, 86400, otelEmitter, ".backup", 1, false, 100);
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
              "test-ofd-job4", null, tableName, 259200, otelEmitter, ".backup", 1, false, 100);
      app.runInner(ops);
      Assertions.assertEquals(259200, app.getTtlSeconds());
    }
  }

  @Test
  public void testOneDayTtlEnabled() throws Exception {
    final String tableName = "db.test_ofd_one_day_ttl_enabled";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      Path orphanFilePath = setupTableWithOrphanFile(ops, tableName, 2);
      ops.spark()
          .sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
                  tableName, AppConstants.OFD_ONE_DAY_TTL_ENABLED_KEY));

      newApp(tableName).runInner(ops);

      Assertions.assertFalse(
          ops.fs().exists(orphanFilePath),
          "Orphan file older than 1 day should be deleted when ofd.one_day_ttl.enabled=true");
    }
  }

  @Test
  public void testDefaultTtl() throws Exception {
    final String tableName = "db.test_ofd_default_ttl";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      Path orphanFilePath = setupTableWithOrphanFile(ops, tableName, 2);

      newApp(tableName).runInner(ops);

      Assertions.assertTrue(
          ops.fs().exists(orphanFilePath),
          "Orphan file younger than the default 7d TTL should be preserved when the property is absent");
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

  private OrphanFilesDeletionSparkApp newApp(String tableName) {
    return new OrphanFilesDeletionSparkApp(
        "test-job-id",
        null,
        tableName,
        DEFAULT_TTL_SECONDS,
        otelEmitter,
        BACKUP_DIR,
        5,
        false,
        20000);
  }

  private Path setupTableWithOrphanFile(Operations ops, String tableName, int orphanAgeDays)
      throws Exception {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark().sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName)).show();
    for (int row = 0; row < 3; ++row) {
      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('v%d', current_timestamp())", tableName, row))
          .show();
    }
    Table table = ops.getTable(tableName);
    Path orphanFilePath = new Path(table.location(), ORPHAN_FILE_NAME);
    FileSystem fs = ops.fs();
    fs.createNewFile(orphanFilePath);
    long mtimeMs = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(orphanAgeDays);
    fs.setTimes(orphanFilePath, mtimeMs, -1);
    return orphanFilePath;
  }
}
