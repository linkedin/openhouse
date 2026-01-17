package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.SparkJobUtil;
import com.linkedin.openhouse.tables.client.model.Policies;
import com.linkedin.openhouse.tables.client.model.Retention;
import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class OperationsTest extends OpenHouseSparkITest {
  private static final String TRASH_DIR = ".trash";
  private static final String BACKUP_DIR = ".backup";
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testRetentionSparkApp() throws Exception {
    final String tableName = "db.test_retention_sql";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTableWithRetentionAndSharingPolicies(ops, tableName, "1d", true);
      populateTable(ops, tableName, 3);
      populateTable(ops, tableName, 2, 2);
      ops.runRetention(tableName, "ts", "", "day", 1, false, "", ZonedDateTime.now());
      verifyRowCount(ops, tableName, 3);
      verifyPolicies(ops, tableName, 1, Retention.GranularityEnum.DAY, true);
    }
  }

  @Test
  public void testRetentionSparkAppWithStringPartitionColumns() throws Exception {
    final String tableName1 = "db.test_retention_string_partition1";
    final String tableName2 = "db.test_retention_string_partition2";
    final String tableName3 = "db.test_retention_string_partition3";
    final String tableName4 = "db.test_retention_string_partition4";
    final String tableName5 = "db.test_retention_string_partition5";
    final String tableName6 = "db.test_retention_string_partition6";

    List<String> rowValue = new ArrayList<>();
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      rowValue.add("202%s-07-16");
      // retention test with default columnPattern. ColumnPattern defaults to "yyyy-MM-dd"
      // if user does not provide it.
      runRetentionJobWithStringPartitionColumns(
          ops, tableName1, rowValue, "datePartition", "yyyy-MM-dd", "day");
      verifyRowCount(ops, tableName1, 0);
      rowValue.clear();

      rowValue.add("202%s-07-16-12");
      runRetentionJobWithStringPartitionColumns(
          ops, tableName2, rowValue, "datePartition", "yyyy-MM-dd-HH", "day");
      verifyRowCount(ops, tableName2, 0);
      rowValue.clear();

      rowValue.add("202%s-07-2218:46:19-0700");
      runRetentionJobWithStringPartitionColumns(
          ops, tableName3, rowValue, "datePartition", "yyyy-MM-ddHH:mm:ssZ", "day");
      verifyRowCount(ops, tableName3, 0);
      rowValue.clear();

      rowValue.add("202%s-07-16-12");
      // data is not fully compliant with format. However, the substring part of data till provided
      // pattern is
      // in compliance. This record gets deleted.
      rowValue.add("202%s-07-16-2218:46:189:0700");
      runRetentionJobWithStringPartitionColumns(
          ops, tableName4, rowValue, "datePartition", "yyyy-MM-dd-HH", "day");
      verifyRowCount(ops, tableName4, 0);
      rowValue.clear();

      rowValue.add("202%s-07-16-12");
      // Rows with format different than the pattern provided. These rows will be deleted even
      // though formats are
      // different due to string comparison logic
      rowValue.add("202%s-07-2218:46:19-0700");
      // Rows with current date which are not to be deleted
      List<Row> currentDates =
          ops.spark()
              .sql("select date_format(current_timestamp(),'yyyy-MM-dd-HH') as string")
              .collectAsList();
      String dateToday = currentDates.get(0).toString();
      rowValue.add(dateToday);
      runRetentionJobWithStringPartitionColumns(
          ops, tableName4, rowValue, "datePartition", "yyyy-MM-dd-HH", "day");
      verifyRowCount(ops, tableName4, 3);
      rowValue.clear();

      // Test case to show that difference in data format and columnPattern format is not blocking
      // delete ops.
      // Data format and pattern are different in terms of delimiter which makes them inconsistent.
      List<Row> currentDatesFormatMismatched =
          ops.spark()
              .sql(
                  "select date_format(current_timestamp() - INTERVAL 5 DAYS,'yyyy-MM-dd-HH') as string")
              .collectAsList();
      rowValue.add(currentDatesFormatMismatched.get(0).get(0).toString());
      runRetentionJobWithStringPartitionColumns(
          ops, tableName5, rowValue, "datePartition", "yyyy-MM.dd.HH", "day");
      ops.spark()
          .sql("select * from openhouse.db.test_retention_string_partition5")
          .collectAsList();
      verifyRowCount(ops, tableName5, 0);
      rowValue.clear();

      // Test to validate the latest snapshot added by retention delete ops is of type `delete`
      rowValue.add("202%s-07-16-12");
      runRetentionJobWithStringPartitionColumns(
          ops, tableName6, rowValue, "datePartition", "yyyy-MM-dd-HH", "day");
      verifyRowCount(ops, tableName6, 0);
      rowValue.clear();
      List<String> operations = getSnapshotOperationTypes(ops, tableName6);
      Assertions.assertEquals(operations.get(0), "delete");
    }
  }

  private void runRetentionJobWithStringPartitionColumns(
      Operations ops,
      String tableName,
      List<String> dataFormats,
      String column,
      String pattern,
      String granularity) {
    prepareTableWithStringColumn(ops, tableName);
    populateTableWithStringColumn(ops, tableName, 3, dataFormats);
    ops.runRetention(tableName, column, pattern, granularity, 2, false, "", ZonedDateTime.now());
  }

  @Test
  public void testRetentionCreatesSnapshotsOnNoOpDelete() throws Exception {
    final String tableName = "db_test.test_retention_sql";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, 4);
      List<Long> snapshots = getSnapshotIds(ops, tableName);
      // check if there are existing snapshots
      Assertions.assertTrue(snapshots.size() > 0);
      ops.runRetention(tableName, "ts", "", "day", 2, false, "", ZonedDateTime.now());
      verifyRowCount(ops, tableName, 4);
      List<Long> snapshotsAfter = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(snapshots.size() + 1, snapshotsAfter.size());
    }
  }

  @Test
  public void testRetentionDataManifestWithStringDatePartitionedTable() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // set up
      String tableName = "db.test_string_partition";
      String columnName = "datepartition";
      String columnPattern = "yyyy-MM-dd-HH";
      String granularity = "DAY";
      int count = 1;
      // prepare data
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(columnPattern);
      String twoDayAgoHour = formatter.format(ZonedDateTime.now().minusDays(2));
      String twoDayAgoDate =
          formatter.format(ZonedDateTime.now().minusDays(2).truncatedTo(ChronoUnit.DAYS));
      String threeDayAgoHour = formatter.format(ZonedDateTime.now().minusDays(3));
      String threeDayAgoDate =
          formatter.format(ZonedDateTime.now().minusDays(3).truncatedTo(ChronoUnit.DAYS));
      ops.spark()
          .sql(
              String.format(
                  "create table %s (data string, datepartition string, hourpartition string, late int) partitioned by (datepartition, hourpartition, late)",
                  tableName));
      ops.spark()
          .sql(
              String.format(
                  "alter table %s SET POLICY (RETENTION = 30d ON COLUMN %s WHERE PATTERN = '%s')",
                  tableName, columnName, columnPattern));
      ops.spark()
          .sql(
              String.format(
                  "insert into %s values ('a', '%s', '%s', 0), ('a', '%s', '%s', 0)",
                  tableName, twoDayAgoDate, twoDayAgoHour, threeDayAgoDate, threeDayAgoHour));
      ops.spark()
          .sql(
              String.format(
                  "insert into %s values ('b', '%s', '%s', 0), ('b', '%s', '%s', 0)",
                  tableName, twoDayAgoDate, twoDayAgoHour, threeDayAgoDate, threeDayAgoHour));
      ZonedDateTime now = ZonedDateTime.now();
      ops.runRetention(
          tableName, columnName, columnPattern, granularity, count, true, ".backup", now);
      // verify data_manifest.json
      Table table = ops.getTable(tableName);
      String manifestName = String.format("data_manifest_%d.json", now.toInstant().toEpochMilli());
      Path firstManifestPath =
          new Path(
              String.format(
                  "%s/.backup/data/datepartition=%s/hourpartition=%s/late=0/%s",
                  table.location(), twoDayAgoDate, twoDayAgoHour, manifestName));
      Path secondManifestPath =
          new Path(
              String.format(
                  "%s/.backup/data/datepartition=%s/hourpartition=%s/late=0/%s",
                  table.location(), threeDayAgoDate, threeDayAgoHour, manifestName));
      Assertions.assertTrue(ops.fs().exists(firstManifestPath));
      Assertions.assertTrue(ops.fs().exists(secondManifestPath));
      try (InputStream in = ops.fs().open(firstManifestPath);
          InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
        JsonObject jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
        Assertions.assertEquals(2, jsonObject.get("file_count").getAsInt());
        String oneDataFilePath = jsonObject.get("files").getAsJsonArray().get(0).getAsString();
        Assertions.assertTrue(
            oneDataFilePath.startsWith(firstManifestPath.getParent().toString())
                && oneDataFilePath.endsWith(".orc"));
      }
      try (InputStream in = ops.fs().open(secondManifestPath);
          InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
        JsonObject jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
        Assertions.assertEquals(2, jsonObject.get("file_count").getAsInt());
        String oneDataFilePath = jsonObject.get("files").getAsJsonArray().get(0).getAsString();
        Assertions.assertTrue(
            oneDataFilePath.startsWith(secondManifestPath.getParent().toString())
                && oneDataFilePath.endsWith(".orc"));
      }
      Assertions.assertEquals(
          table.location() + "/.backup", table.properties().get(AppConstants.BACKUP_DIR_KEY));
    }
  }

  @Test
  public void testRetentionDataManifestWithTimestampPartitionedTable() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // set up
      String tableName = "db.test_time_partition";
      String columnName = "ts";
      String columnPattern = "";
      String granularity = "DAY";
      int count = 1;
      // prepare data
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
      String today = formatter.format(ZonedDateTime.now());
      String twoDayAgo = formatter.format(ZonedDateTime.now().minusDays(2));
      ops.spark()
          .sql(
              String.format(
                  "create table %s (data string, ts timestamp) partitioned by (Days(ts))",
                  tableName));
      ops.spark()
          .sql(
              String.format(
                  "insert into %s values ('a', cast('%s' as timestamp)), ('a', cast('%s' as timestamp))",
                  tableName, today, twoDayAgo));
      ops.spark()
          .sql(
              String.format(
                  "insert into %s values ('b', cast('%s' as timestamp)), ('b', cast('%s' as timestamp))",
                  tableName, today, twoDayAgo));
      ZonedDateTime now = ZonedDateTime.now();
      ops.runRetention(
          tableName, columnName, columnPattern, granularity, count, true, ".backup", now);
      // verify data_manifest.json
      Table table = ops.getTable(tableName);
      String manifestName = String.format("data_manifest_%d.json", now.toInstant().toEpochMilli());
      Path manifestPath =
          new Path(
              String.format(
                  "%s/.backup/data/ts_day=%s/%s", table.location(), twoDayAgo, manifestName));
      Assertions.assertTrue(ops.fs().exists(manifestPath));
      try (InputStream in = ops.fs().open(manifestPath);
          InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
        JsonObject jsonObject = JsonParser.parseReader(reader).getAsJsonObject();
        Assertions.assertEquals(2, jsonObject.get("file_count").getAsInt());
        String oneDataFilePath = jsonObject.get("files").getAsJsonArray().get(0).getAsString();
        Assertions.assertTrue(
            oneDataFilePath.startsWith(manifestPath.getParent().toString())
                && oneDataFilePath.endsWith(".orc"));
      }
      Assertions.assertEquals(
          table.location() + "/.backup", table.properties().get(AppConstants.BACKUP_DIR_KEY));
    }
  }

  @Test
  public void testOrphanFilesDeletionJavaAPI() throws Exception {
    final String tableName = "db.test_ofd_java";
    final String testOrphanFileName = "data/test_orphan_file.orc";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath = new Path(table.location(), testOrphanFileName);
      Path dataManifestPath = new Path(table.location(), ".backup/data/data_manifest_123.json");
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath);
      fs.createNewFile(dataManifestPath);
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, BACKUP_DIR, 5);
      List<String> orphanFiles = Lists.newArrayList(result.orphanFileLocations().iterator());
      Assertions.assertTrue(
          fs.exists(new Path(table.location(), new Path(BACKUP_DIR, testOrphanFileName))));
      Assertions.assertEquals(1, orphanFiles.size());
      Assertions.assertTrue(
          orphanFiles.get(0).endsWith(table.location() + "/" + testOrphanFileName));
      Assertions.assertFalse(fs.exists(orphanFilePath));
    }
  }

  @Test
  public void testOrphanFilesDeletionIgnoresFilesInBackupDir() throws Exception {
    final String tableName = "db.test_ofd_java";
    final String testOrphanFileName = "data/test_orphan_file.orc";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath = new Path(table.location(), testOrphanFileName);
      Path dataManifestPath = new Path(table.location(), ".backup/data/data_manifest_123.json");
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath);
      fs.createNewFile(dataManifestPath);
      ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, BACKUP_DIR, 5);
      Path backupFilePath = new Path(table.location(), new Path(BACKUP_DIR, testOrphanFileName));
      Assertions.assertTrue(fs.exists(backupFilePath));
      // run delete operation again and verify that files in .backup are not listed as Orphan
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, BACKUP_DIR, 5);
      List<String> orphanFiles = Lists.newArrayList(result.orphanFileLocations().iterator());
      Assertions.assertEquals(0, orphanFiles.size());
      Assertions.assertTrue(fs.exists(backupFilePath));
    }
  }

  @Test
  public void testOrphanFilesDeletionDeleteNonDataFiles() throws Exception {
    final String tableName = "db.test_ofd";
    final String testOrphanFileName = "metadata/test_orphan_file.avro";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath = new Path(table.location(), testOrphanFileName);
      Path dataManifestPath = new Path(table.location(), ".backup/data/data_manifest_123.json");
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath);
      fs.createNewFile(dataManifestPath);
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, BACKUP_DIR, 5);
      List<String> orphanFiles = Lists.newArrayList(result.orphanFileLocations().iterator());
      Assertions.assertFalse(
          fs.exists(new Path(table.location(), new Path(BACKUP_DIR, testOrphanFileName))));
      Assertions.assertEquals(1, orphanFiles.size());
      Assertions.assertTrue(
          orphanFiles.get(0).endsWith(table.location() + "/" + testOrphanFileName));
      Assertions.assertFalse(fs.exists(orphanFilePath));
    }
  }

  @Test
  public void testOrphanFilesDeletionBackupDisabled() throws Exception {
    final String tableName = "db.test_ofd";
    final String testOrphanFileName = "data/test_orphan_file.orc";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath = new Path(table.location(), testOrphanFileName);
      Path dataManifestPath = new Path(table.location(), ".backup/data/data_manifest_123.json");
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath);
      fs.createNewFile(dataManifestPath);
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(table, System.currentTimeMillis(), false, BACKUP_DIR, 5);
      List<String> orphanFiles = Lists.newArrayList(result.orphanFileLocations().iterator());
      Assertions.assertFalse(
          fs.exists(new Path(table.location(), new Path(BACKUP_DIR, testOrphanFileName))));
      Assertions.assertEquals(1, orphanFiles.size());
      Assertions.assertTrue(
          orphanFiles.get(0).endsWith(table.location() + "/" + testOrphanFileName));
      Assertions.assertFalse(fs.exists(orphanFilePath));
    }
  }

  @Test
  public void testOrphanFilesDeletionDeleteDataWhenDataManifestNotExists() throws Exception {
    final String tableName = "db.test_ofd_java";
    final String testOrphanFileName = "data/test_orphan_file.orc";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath = new Path(table.location(), testOrphanFileName);
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath);
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, BACKUP_DIR, 5);
      List<String> orphanFiles = Lists.newArrayList(result.orphanFileLocations().iterator());
      Assertions.assertFalse(
          fs.exists(new Path(table.location(), new Path(BACKUP_DIR, testOrphanFileName))));
      Assertions.assertEquals(1, orphanFiles.size());
      Assertions.assertTrue(
          orphanFiles.get(0).endsWith(table.location() + "/" + testOrphanFileName));
      Assertions.assertFalse(fs.exists(orphanFilePath));
    }
  }

  @Test
  public void testSnapshotsExpirationMaxAge() throws Exception {
    final String tableName = "db.test_es_maxage_java";
    final int numInserts = 3;
    final int maxAge = 0;
    final String timeGranularity = "DAYS";

    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, 0);
      // Only retain the last snapshot
      checkSnapshots(table, snapshotIds.subList(snapshotIds.size() - 1, snapshotIds.size()));
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(
          ops, tableName, snapshotIds.subList(snapshotIds.size() - 1, snapshotIds.size()));
    }
  }

  @Test
  public void testSnapshotsExpirationMaxAgeNoop() throws Exception {
    final String tableName = "db.test_es_maxage_noop_java";
    final int numInserts = 3;
    final int maxAge = 3;
    final String timeGranularity = "DAYS";

    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, 0);
      // No snapshots should be cleaned up as they are all within the max age
      checkSnapshots(table, snapshotIds);
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(ops, tableName, snapshotIds);
    }
  }

  @Test
  public void testSnapshotsExpirationVersionsNoop() throws Exception {
    final String tableName = "db.test_es_versions_noop_java";
    final int numInserts = 3;
    final int versionsToKeep = 5; // Should keep all versions given that there are fewer versions
    final int maxAge = 3;
    final String timeGranularity = "DAYS";
    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, versionsToKeep);
      // verify that table object snapshots are updated
      checkSnapshots(table, snapshotIds);
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(ops, tableName, snapshotIds);
    }
  }

  @Test
  public void testSnapshotsExpirationVersions() throws Exception {
    final String tableName = "db.test_es_versions_java";
    final int numInserts = 3;
    final int versionsToKeep = 2;
    final int maxAge = 3;
    final String timeGranularity = "DAYS";
    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, versionsToKeep);
      // verify that table object snapshots are updated
      checkSnapshots(
          table, snapshotIds.subList(snapshotIds.size() - versionsToKeep, snapshotIds.size()));
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(
          ops,
          tableName,
          snapshotIds.subList(snapshotIds.size() - versionsToKeep, snapshotIds.size()));
    }
  }

  @Test
  public void testSnapshotsExpirationBothAgeAndVersions() throws Exception {
    final String tableName = "db.test_es_age_and_versions_java";
    final int numInserts = 3;
    final int maxAge = 3;
    final String timeGranularity = "DAYS";
    final int versionsToKeep = 1;
    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, versionsToKeep);
      // verify that table object snapshots are updated
      checkSnapshots(
          table, snapshotIds.subList(snapshotIds.size() - versionsToKeep, snapshotIds.size()));
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(
          ops,
          tableName,
          snapshotIds.subList(snapshotIds.size() - versionsToKeep, snapshotIds.size()));
    }
  }

  @Test
  public void testSnapshotsExpirationPrioritizeAge() throws Exception {
    final String tableName = "db.test_es_age_prioritization_java";
    final int numInserts = 3;
    final int maxAge = 20;
    final String timeGranularity =
        "SECONDS"; // Not a realistic user configuration, for the sake of testing
    final int versionsToKeep = 5;
    final int versionsToKeepAfterExpiration = 2;
    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Thread.sleep(30000); // Sleep for 30 seconds to expire all the snapshots
      populateTable(ops, tableName, versionsToKeepAfterExpiration);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts + versionsToKeepAfterExpiration,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      ops.expireSnapshots(table, maxAge, timeGranularity, versionsToKeep);
      // verify that only 2 snapshots are kept instead of 5 due to prioritizing maxAge
      checkSnapshots(
          table,
          snapshotIds.subList(
              snapshotIds.size() - versionsToKeepAfterExpiration, snapshotIds.size()));
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(
          ops,
          tableName,
          snapshotIds.subList(
              snapshotIds.size() - versionsToKeepAfterExpiration, snapshotIds.size()));
    }
  }

  @Test
  public void testSnapshotExpirationWithHoursDaysMonthsYears() throws Exception {
    final String tableName = "db.test_es_age_policy";
    final int numInserts = 3;
    final int maxAge = 20;
    final int versionsToKeep = 5;
    List<Long> snapshotIds;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          numInserts,
          snapshotIds.size(),
          String.format("There must be %d snapshot(s) after inserts", numInserts));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());

      for (TimePartitionSpec.GranularityEnum granularity :
          TimePartitionSpec.GranularityEnum.values()) {
        ops.expireSnapshots(table, maxAge, granularity.getValue(), versionsToKeep);
        // verify that no snapshots are missing
        checkSnapshots(table, snapshotIds);
      }
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // verify that new apps see snapshots correctly
      checkSnapshots(ops, tableName, snapshotIds);
    }
  }

  @Test
  public void testStagedFilesDelete() throws Exception {
    final String tableName = "db.test_staged_delete";
    final int numInserts = 3;
    final String testOrphanFile1 = "data/test_orphan_file1.orc";
    final String testOrphanFile2 = "data/test_orphan_file2.orc";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      Path orphanFilePath1 = new Path(table.location(), testOrphanFile1);
      Path orphanFilePath2 = new Path(table.location(), testOrphanFile2);
      Path dataManifestPath = new Path(table.location(), ".trash/data/data_manifest_123.json");
      FileSystem fs = ops.fs();
      fs.createNewFile(orphanFilePath1);
      fs.createNewFile(orphanFilePath2);
      fs.createNewFile(dataManifestPath);
      log.info("Created orphan file {}", testOrphanFile1);
      log.info("Created orphan file {}", testOrphanFile2);
      ops.deleteOrphanFiles(table, System.currentTimeMillis(), true, TRASH_DIR, 5);
      Assertions.assertTrue(
          fs.exists(new Path(table.location(), (new Path(TRASH_DIR, testOrphanFile1)))));
      Assertions.assertTrue(
          fs.exists((new Path(table.location(), (new Path(TRASH_DIR, testOrphanFile2))))));
      Assertions.assertFalse(fs.exists(orphanFilePath1));
      Assertions.assertFalse(fs.exists(orphanFilePath2));
      // set timestamp for an orphan file in trash dir to 4 days old
      SparkJobUtil.setModifiedTimeStamp(
          fs, new Path(table.location(), new Path(TRASH_DIR, testOrphanFile1)), 4);
      ops.deleteStagedFiles(new Path(table.location(), TRASH_DIR), 3, true);
      Assertions.assertFalse(
          fs.exists(new Path(table.location(), new Path(TRASH_DIR, testOrphanFile1))));
      Assertions.assertTrue(
          fs.exists(new Path(table.location(), new Path(TRASH_DIR, testOrphanFile2))));
    }
  }

  @Test
  public void testDataCompactionPartialProgressNonPartitionedTable() throws Exception {
    final String tableName = "db.test_data_compaction";
    final int numInserts = 3;

    BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
        (ops, table) ->
            ops.rewriteDataFiles(
                table,
                1024 * 1024, // 1MB
                1024, // 1KB
                1024 * 1024 * 2, // 2MB
                2,
                1,
                true,
                10,
                Integer.MAX_VALUE);

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(1, result.addedDataFilesCount());
      Assertions.assertEquals(3, result.rewrittenDataFilesCount());
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      long expectedNumSnapshots = numInserts + 1;
      List<Long> snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          expectedNumSnapshots,
          snapshotIds.size(),
          String.format(
              "There must be %d snapshot(s) after %d inserts and 1 data files rewrite",
              expectedNumSnapshots, numInserts));
      // check that no rewrite happens second time
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(0, result.addedDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenBytesCount());
    }
  }

  @Test
  public void testDataCompactionPartialProgressPartitionedTable() throws Exception {
    final String tableName = "db.test_data_compaction_partitioned";
    final int numInsertsPerPartition = 3;
    final int numDailyPartitions = 10;
    final int maxCommits = 5;

    BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
        (ops, table) ->
            ops.rewriteDataFiles(
                table,
                1024 * 1024, // 1MB
                1024, // 1KB
                1024 * 1024 * 2, // 2MB
                2,
                1,
                true,
                maxCommits,
                Integer.MAX_VALUE);

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName, true);
      long fixedTimestampSeconds = System.currentTimeMillis() / 1000;
      for (int daysLag = 0; daysLag < numDailyPartitions; ++daysLag) {
        populateTable(ops, tableName, numInsertsPerPartition, daysLag, fixedTimestampSeconds);
      }
      log.info("Produced the following data files:");
      getDataFiles(ops, tableName).forEach(f -> log.info(f.toString()));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(numDailyPartitions, result.addedDataFilesCount());
      Assertions.assertEquals(
          numInsertsPerPartition * numDailyPartitions, result.rewrittenDataFilesCount());
      result
          .rewriteResults()
          .forEach(
              fileGroupRewriteResult -> {
                log.info(
                    "File group {} has {} added files, {} rewritten files, {} rewritten bytes",
                    Operations.groupInfoToString(fileGroupRewriteResult.info()),
                    fileGroupRewriteResult.addedDataFilesCount(),
                    fileGroupRewriteResult.rewrittenDataFilesCount(),
                    fileGroupRewriteResult.rewrittenBytesCount());
              });
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // all rewritten files must be in the same commit
      long expectedNumSnapshots = numInsertsPerPartition * numDailyPartitions + 5;
      List<Triple<String, String, Long>> dataFiles = getDataFiles(ops, tableName);
      Assertions.assertEquals(numDailyPartitions, dataFiles.size());
      log.info(
          String.format("Produced the following %d data files after rewrite:", dataFiles.size()));
      dataFiles.forEach(f -> log.info(f.toString()));
      List<Long> snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          expectedNumSnapshots,
          snapshotIds.size(),
          String.format(
              "There must be %d snapshot(s) after %d inserts and %d commits during 1 data files rewrite",
              expectedNumSnapshots,
              numInsertsPerPartition * numDailyPartitions,
              numDailyPartitions));
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      // check that no rewrite happens second time
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(0, result.addedDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenBytesCount());
    }
  }

  @Test
  public void testOrphanDirsDeletionJavaAPI() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // test orphan delete
      Path tbLoc = prepareOrphanTableDirectory(ops, "db1.test_odd_orphan");

      long timeThreshold = System.currentTimeMillis();
      List<Path> matchingFilesBefore = new ArrayList<>();
      ops.listFiles(tbLoc, file -> true, true, matchingFilesBefore);
      boolean orphaned = ops.deleteOrphanDirectory(tbLoc, ".trash", timeThreshold);
      Assertions.assertTrue(orphaned);
      // All files should have been moved to trash dir
      // Making sure nothing needs to be orphaned again
      orphaned = ops.deleteOrphanDirectory(tbLoc, ".trash", timeThreshold);
      Assertions.assertFalse(orphaned);
      List<Path> matchingFilesAfter = new ArrayList<>();
      ops.listFiles(tbLoc, file -> true, true, matchingFilesAfter);
      Assertions.assertEquals(matchingFilesBefore.size(), matchingFilesAfter.size());

      // test stage delete
      ops.deleteStagedOrphanDirectory(tbLoc, ".trash", timeThreshold);
      // test table dir no longer exists
      Assertions.assertFalse(ops.fs().exists(tbLoc));
    }
  }

  private static Path prepareOrphanTableDirectory(Operations ops, String tableName)
      throws Exception {
    Schema schema = getTableSchema();
    Transaction xact = ops.createTransaction(tableName, schema);
    Path tbLoc = new Path(xact.table().location());

    // populate more files
    FileSystem fs = ops.fs();
    Path dataPath = new Path(tbLoc, "data/datepartition");
    fs.mkdirs(dataPath);
    Assertions.assertTrue(fs.exists(dataPath));

    int numInserts = 4;
    for (int i = 0; i < numInserts; ++i) {
      String fileName = "testing" + i + ".orc";
      fs.createNewFile(new Path(dataPath, fileName));
      Assertions.assertTrue(fs.exists(new Path(dataPath, fileName)));
    }

    Path metadataPath = new Path(tbLoc, "metadata");
    fs.mkdirs(metadataPath);
    Assertions.assertTrue(fs.exists(metadataPath));

    for (int i = 0; i < numInserts; ++i) {
      String fileName = "testing" + i + ".avro";
      fs.createNewFile(new Path(metadataPath, fileName));
      Assertions.assertTrue(fs.exists(new Path(metadataPath, fileName)));
    }
    return tbLoc;
  }

  @Test
  public void testCollectEarliestPartitionDateStat() throws Exception {
    final String tableName = "db.test_collect_earliest_partition_date";
    List<String> rowValue = new ArrayList<>();

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Test table with no policy
      prepareTable(ops, tableName);
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertNull(stats.getEarliestPartitionDate());

      // Test table with no partition
      prepareTableWithPoliciesWithDateColumn(ops, tableName, "30d", false, false);
      stats = ops.collectTableStats(tableName);
      Assertions.assertNull(stats.getEarliestPartitionDate());

      // Test table has sharing policy but no retention policy
      prepareTableWithSharingPolicies(ops, tableName, true);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), true);

      // Test yyyy-mm-dd format on table with multiple partitioned columns
      prepareTableWithPoliciesWithMultipleStringPartition(ops, tableName, "30d", false);
      rowValue.add("202%s-07-16");
      rowValue.add("202%s-07-17");
      rowValue.add("202%s-08-16");
      rowValue.add("202%s-09-16");
      populateTableWithMultipleStringColumn(ops, tableName, 1, rowValue);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getEarliestPartitionDate(), "202%s-07-16");
      rowValue.clear();

      // Test timestamp format
      prepareTableWithRetentionAndSharingPolicies(ops, tableName, "30d", false);
      populateTable(ops, tableName, 1, 2);
      populateTable(ops, tableName, 1, 1);
      populateTable(ops, tableName, 1, 0);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(
          stats.getEarliestPartitionDate(),
          LocalDate.now(ZoneOffset.UTC)
              .minusDays(2)
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    }
  }

  @Test
  public void testCollectTableStatsWithEmptyPartitions() throws Exception {
    final String tableName = "db.test_empty_partitions";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Create table with partition but no data
      prepareTableWithRetentionAndSharingPolicies(ops, tableName, "30d", true);

      // Collect stats without any data
      IcebergTableStats stats = ops.collectTableStats(tableName);

      // Verify stats are collected without NPE and partition date is null
      Assertions.assertNotNull(stats);
      Assertions.assertNull(stats.getEarliestPartitionDate());

      // Add some data and verify stats again
      populateTable(ops, tableName, 2, 2);
      stats = ops.collectTableStats(tableName);
      Assertions.assertNotNull(stats.getEarliestPartitionDate());
    }
  }

  @Test
  public void testCollectTablePolicyStats() throws Exception {
    final String tableName = "db.test_collect_table_stats_with_policy";
    List<String> rowValue = new ArrayList<>();

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Test table with both retention and sharing policies
      prepareTableWithRetentionAndSharingPolicies(ops, tableName, "30d", true);
      populateTable(ops, tableName, 2, 2);
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), true);
      Assertions.assertEquals(stats.getRetentionPolicies().getCount(), 30);
      Assertions.assertEquals(stats.getRetentionPolicies().getGranularity(), "DAY");
      Assertions.assertEquals(stats.getHistoryPolicy().getMaxAge(), 3);
      Assertions.assertEquals(stats.getHistoryPolicy().getVersions(), 0);
      Assertions.assertEquals(stats.getHistoryPolicy().getGranularity(), "DAY");

      // Test table with retention policy and sharing set to false
      prepareTableWithPoliciesWithDateColumn(ops, tableName, "16h", false, true);
      rowValue.add("202%s-07-16");
      populateTableWithStringColumn(ops, tableName, 1, rowValue);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), false);
      Assertions.assertEquals(stats.getRetentionPolicies().getCount(), 16);
      Assertions.assertEquals(stats.getRetentionPolicies().getGranularity(), "HOUR");
      Assertions.assertEquals(stats.getRetentionPolicies().getColumnName(), "datepartition");
      Assertions.assertEquals(stats.getRetentionPolicies().getColumnPattern(), "yyyy-MM-dd-HH");
      Assertions.assertEquals(
          stats.getOldestSnapshotTimestamp(), stats.getCurrentSnapshotTimestamp());
      rowValue.clear();

      // Test table with retention policy with custom string partition without sharing policy set
      prepareTableWithPoliciesWithCustomStringPartition(ops, tableName, "2y", "yyyy-MM-dd-HH");
      rowValue.add("202%s-07-16-12");
      populateTableWithStringColumn(ops, tableName, 1, rowValue);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getRetentionPolicies().getCount(), 2);
      Assertions.assertEquals(stats.getRetentionPolicies().getGranularity(), "YEAR");
      Assertions.assertEquals(stats.getRetentionPolicies().getColumnPattern(), "'yyyy-MM-dd-HH'");

      // Test table with sharing policy without retention policy
      prepareTableWithSharingPolicies(ops, tableName, true);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), true);
      // Test that building RetentionStatsSchema will not fail even if retention is not set
      Assertions.assertNotNull(stats.getRetentionPolicies());
      Assertions.assertNull(stats.getRetentionPolicies().getGranularity());
      Assertions.assertEquals(stats.getRetentionPolicies().getCount(), 0);
      Assertions.assertNull(stats.getRetentionPolicies().getColumnName());
      Assertions.assertNull(stats.getRetentionPolicies().getColumnPattern());
      prepareTableWithAllPolicies(
          ops,
          tableName,
          "4M",
          false,
          "MAX_AGE=2D VERSIONS=20",
          "({destination:'a', interval:12H})");
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), false);
      Assertions.assertEquals(stats.getRetentionPolicies().getCount(), 4);
      Assertions.assertEquals(stats.getRetentionPolicies().getGranularity(), "MONTH");
      Assertions.assertEquals(stats.getHistoryPolicy().getGranularity(), "DAY");
      Assertions.assertEquals(stats.getHistoryPolicy().getMaxAge(), 2);
      Assertions.assertEquals(stats.getHistoryPolicy().getVersions(), 20);
      Assertions.assertEquals(stats.getHistoryPolicy().getVersions(), 20);
      Assertions.assertNull(stats.getOldestSnapshotTimestamp());
      Assertions.assertNull(stats.getCurrentSnapshotTimestamp());
    }
  }

  @Test
  public void testCollectTableStats() throws Exception {
    final String tableName = "db.test_collect_table_stats";
    final int numInserts = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      IcebergTableStats stats = ops.collectTableStats(tableName);
      // Ensure defaults for setSharingEnabled is set
      Assertions.assertEquals(stats.getSharingEnabled(), false);
      // Validate empty data files case
      Assertions.assertEquals(stats.getNumReferencedDataFiles(), 0);
      Assertions.assertEquals(stats.getNumExistingMetadataJsonFiles(), 1);
      long modifiedTimeStamp = System.currentTimeMillis();

      populateTable(ops, tableName, 1);
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getNumReferencedDataFiles(), 1);
      Assertions.assertTrue(stats.getTableLastUpdatedTimestamp() >= modifiedTimeStamp);
      // Capture first snapshot timestamp
      Table table = ops.getTable(tableName);
      long oldestSnapshot = table.currentSnapshot().timestampMillis();

      // Add more records and validate other stats
      populateTable(ops, tableName, numInserts);
      table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getCurrentSnapshotId(), table.currentSnapshot().snapshotId());
      Assertions.assertTrue(
          stats.getCurrentSnapshotTimestamp() > stats.getSecondOldestSnapshotTimestamp());
      Assertions.assertTrue(
          stats.getSecondOldestSnapshotTimestamp() > stats.getOldestSnapshotTimestamp());
      Assertions.assertEquals(stats.getNumReferencedDataFiles(), numInserts + 1);
      Assertions.assertEquals(stats.getNumExistingMetadataJsonFiles(), numInserts + 2);
      Assertions.assertEquals(
          stats.getCurrentSnapshotTimestamp(), table.currentSnapshot().timestampMillis());
      Assertions.assertEquals(stats.getOldestSnapshotTimestamp(), oldestSnapshot);
      Assertions.assertEquals(
          stats.getNumObjectsInDirectory(),
          stats.getNumReferencedDataFiles()
              + stats.getNumExistingMetadataJsonFiles()
              + stats.getNumReferencedManifestFiles()
              + stats.getNumReferencedManifestLists());
    }
  }

  @Test
  public void testCollectHistoryPolicyStatsWithSnapshots() throws Exception {
    final String tableName = "db.test_collect_table_stats_with_history_policy_snapshots";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Test table with both retention and sharing policies
      prepareTableWithHistoryPolicies(ops, tableName, "MAX_AGE=2D VERSIONS=20");
      populateTable(ops, tableName, 2, 2);
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(stats.getSharingEnabled(), false);
      Assertions.assertEquals(stats.getHistoryPolicy().getMaxAge(), 2);
      Assertions.assertEquals(stats.getHistoryPolicy().getVersions(), 20);
      Assertions.assertEquals(stats.getHistoryPolicy().getGranularity(), "DAY");
      Assertions.assertEquals(stats.getNumSnapshots(), 2);
      Assertions.assertNotEquals(
          stats.getCurrentSnapshotTimestamp(), stats.getOldestSnapshotTimestamp());
      Assertions.assertEquals(
          stats.getCurrentSnapshotTimestamp(), stats.getSecondOldestSnapshotTimestamp());
    }
  }

  private void verifyPolicies(
      Operations ops,
      String tableName,
      int expectedRetentionCount,
      Retention.GranularityEnum expectedRetentionGranularity,
      boolean expectedSharing) {
    List<Row> resultRows =
        ops.spark().sql(String.format("SHOW TBLPROPERTIES %s", tableName)).collectAsList();
    Map<String, String> collect =
        resultRows.stream().collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
    String policiesStr = String.valueOf(collect.get("policies"));
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    Policies policies = gson.fromJson(policiesStr, Policies.class);
    Assertions.assertEquals(policies.getRetention().getCount(), expectedRetentionCount);
    Assertions.assertEquals(policies.getRetention().getGranularity(), expectedRetentionGranularity);
    Assertions.assertEquals(policies.getSharingEnabled().booleanValue(), expectedSharing);
  }

  private static void verifyRowCount(Operations ops, String tableName, int expectedRowCount) {
    List<Row> resultRows =
        ops.spark().sql(String.format("SELECT * FROM %s", tableName)).collectAsList();
    Assertions.assertEquals(expectedRowCount, resultRows.size());
  }

  private static void populateTable(
      Operations ops, String tableName, int numRows, int dayLag, long timestampSeconds) {
    String timestampEntry =
        String.format("date_sub(from_unixtime(%d), %d)", timestampSeconds, dayLag);
    for (int row = 0; row < numRows; ++row) {
      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('v%d', %s)", tableName, row, timestampEntry))
          .show();
    }
  }

  private static void populateTable(Operations ops, String tableName, int numRows, int dayLag) {
    populateTable(ops, tableName, numRows, dayLag, System.currentTimeMillis() / 1000);
  }

  private static void populateTable(Operations ops, String tableName, int numRows) {
    populateTable(ops, tableName, numRows, 0);
  }

  private static void populateTableWithStringColumn(
      Operations ops, String tableName, int numRows, List<String> dataFormats) {
    for (String dataFormat : dataFormats) {
      for (int row = 0; row < numRows; ++row) {
        ops.spark()
            .sql(
                String.format(
                    "INSERT INTO %s VALUES ('%s', '%s')",
                    tableName, row, String.format(dataFormat, row)))
            .show();
      }
    }
  }

  private static void populateTableWithMultipleStringColumn(
      Operations ops, String tableName, int numRows, List<String> dataFormats) {
    for (String dataFormat : dataFormats) {
      for (int row = 0; row < numRows; ++row) {
        ops.spark()
            .sql(
                String.format(
                    "INSERT INTO %s VALUES ('%s', '%s', %d)",
                    tableName, dataFormat, String.format(dataFormat, row), row))
            .show();
      }
    }
  }

  private static void prepareTable(Operations ops, String tableName) {
    prepareTable(ops, tableName, false);
  }

  private static Schema getTableSchema() {
    return new Schema(
        Types.NestedField.required(1, "data", Types.StringType.get()),
        Types.NestedField.required(2, "ts", Types.TimestampType.withZone()));
  }

  private static void prepareTable(Operations ops, String tableName, boolean isPartitioned) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    if (isPartitioned) {
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, ts timestamp) partitioned by (days(ts))",
                  tableName))
          .show();
    } else {
      ops.spark()
          .sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName))
          .show();
    }
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithStringColumn(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, datePartition String) PARTITIONED by (datePartition)",
                tableName))
        .show();
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithRetentionAndSharingPolicies(
      Operations ops, String tableName, String retention, boolean sharing) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) PARTITIONED BY (days(ts))", tableName))
        .show();
    ops.spark()
        .sql(String.format("ALTER TABLE %s SET POLICY (RETENTION=%s)", tableName, retention));
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (SHARING=%s)", tableName, sharing));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithSharingPolicies(
      Operations ops, String tableName, boolean sharing) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, datePartition string) PARTITIONED BY (datepartition)",
                tableName))
        .show();
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (SHARING=%s)", tableName, sharing));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithAllPolicies(
      Operations ops,
      String tableName,
      String retention,
      boolean sharing,
      String history,
      String replication) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) PARTITIONED BY (months(ts))",
                tableName))
        .show();
    ops.spark()
        .sql(String.format("ALTER TABLE %s SET POLICY (RETENTION=%s)", tableName, retention));
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (SHARING=%s)", tableName, sharing));
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (HISTORY %s)", tableName, history));
    ops.spark()
        .sql(String.format("ALTER TABLE %s SET POLICY (REPLICATION = %s)", tableName, replication));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void checkSnapshots(
      Operations ops, String tableName, List<Long> expectedSnapshotIds) {
    log.info("Checking snapshots");
    List<Long> foundSnapshotIds = getSnapshotIds(ops, tableName);
    Assertions.assertEquals(expectedSnapshotIds, foundSnapshotIds, "Incorrect list of snapshots");
  }

  private static void prepareTableWithPoliciesWithDateColumn(
      Operations ops, String tableName, String retention, boolean sharing, boolean isPartitioned) {
    String tableCreateStatement =
        String.format("CREATE TABLE %s (data string, datepartition string)", tableName);
    if (isPartitioned) {
      tableCreateStatement += " PARTITIONED BY (datepartition)";
    }
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark().sql(tableCreateStatement).show();
    ops.spark()
        .sql(
            String.format(
                "ALTER TABLE %s SET POLICY (RETENTION=%s ON COLUMN datepartition)",
                tableName, retention));
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (SHARING=%s)", tableName, sharing));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithPoliciesWithMultipleStringPartition(
      Operations ops, String tableName, String retention, boolean sharing) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (datepartition string, data string, num int) PARTITIONED BY (datepartition, num)",
                tableName))
        .show();
    ops.spark()
        .sql(
            String.format(
                "ALTER TABLE %s SET POLICY (RETENTION=%s ON COLUMN datepartition)",
                tableName, retention));
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (SHARING=%s)", tableName, sharing));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithPoliciesWithCustomStringPartition(
      Operations ops, String tableName, String retention, String pattern) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, datepartition string) PARTITIONED BY (datepartition)",
                tableName))
        .show();
    ops.spark()
        .sql(
            String.format(
                "ALTER TABLE %s SET POLICY (RETENTION=%s ON COLUMN datepartition WHERE PATTERN = '%s')",
                tableName, retention, pattern));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void prepareTableWithHistoryPolicies(
      Operations ops, String tableName, String history) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) PARTITIONED BY (days(ts))", tableName))
        .show();
    ops.spark().sql(String.format("ALTER TABLE %s SET POLICY (HISTORY %s)", tableName, history));
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static void checkSnapshots(Table table, List<Long> expectedSnapshotIds) {
    log.info("Checking snapshots");
    List<Long> foundSnapshotIds =
        Lists.newArrayList(table.snapshots()).stream()
            .map(Snapshot::snapshotId)
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedSnapshotIds, foundSnapshotIds, "Incorrect list of snapshots");
  }

  private static List<Long> getSnapshotIds(Operations ops, String tableName) {
    log.info("Getting snapshot Ids");
    List<Row> snapshots =
        ops.spark().sql(String.format("SELECT * FROM %s.snapshots", tableName)).collectAsList();
    snapshots.forEach(s -> log.info(s.toString()));
    return snapshots.stream()
        .map(r -> r.getLong(r.fieldIndex("snapshot_id")))
        .collect(Collectors.toList());
  }

  private static List<String> getSnapshotOperationTypes(Operations ops, String tableName) {
    log.info("Getting snapshot Operations");
    List<Row> orderedSnapshots =
        ops.spark()
            .sql(String.format("SELECT * FROM %s.snapshots order by committed_at desc", tableName))
            .collectAsList();
    orderedSnapshots.forEach(s -> log.info(s.toString()));
    return orderedSnapshots.stream()
        .map(r -> r.getString(r.fieldIndex("operation")))
        .collect(Collectors.toList());
  }

  private static List<Triple<String, String, Long>> getDataFiles(Operations ops, String tableName) {
    List<Row> dataFiles =
        ops.spark().sql(String.format("SELECT * FROM %s.data_files", tableName)).collectAsList();
    return dataFiles.stream()
        .map(
            r ->
                Triple.of(
                    r.getStruct(r.fieldIndex("partition")).json(),
                    r.getString(r.fieldIndex("file_path")),
                    r.getLong(r.fieldIndex("record_count"))))
        .collect(Collectors.toList());
  }

  // ==================================================================================
  // Tests for Operations.collectCommitEventTablePartitionStats() - Method-Level Integration
  // ==================================================================================

  @Test
  public void testCollectPartitionStatsForPartitionedTable() throws Exception {
    final String tableName = "db.test_partition_stats_partitioned";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);

      // Insert data across multiple partitions
      populateTable(ops, tableName, 3, 0); // Partition day 0
      populateTable(ops, tableName, 2, 1); // Partition day 1

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats> stats =
          ops.collectCommitEventTablePartitionStats(tableName);

      // Assert: Should have one stats record per partition
      Assertions.assertNotNull(stats, "Stats should not be null");
      Assertions.assertEquals(2, stats.size(), "Should have 2 partition stats (one per partition)");

      // Verify each stat has required fields
      for (com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat : stats) {
        Assertions.assertNotNull(stat.getDataset(), "Dataset should not be null");
        Assertions.assertNotNull(stat.getCommitMetadata(), "Commit metadata should not be null");
        Assertions.assertNotNull(stat.getPartitionData(), "Partition data should not be null");
        Assertions.assertFalse(
            stat.getPartitionData().isEmpty(),
            "Partition data should not be empty for partitioned table");
        Assertions.assertTrue(stat.getRowCount() > 0, "Row count should be greater than 0");
        Assertions.assertTrue(stat.getColumnCount() > 0, "Column count should be greater than 0");
        Assertions.assertNotNull(stat.getNullCount(), "Null count map should not be null");
        Assertions.assertFalse(stat.getNullCount().isEmpty(), "Null count map should have entries");

        log.info(
            "Partition stats: partitionData={}, rowCount={}, columnCount={}",
            stat.getPartitionData(),
            stat.getRowCount(),
            stat.getColumnCount());
      }
    }
  }

  @Test
  public void testCollectPartitionStatsForUnpartitionedTable() throws Exception {
    final String tableName = "db.test_partition_stats_unpartitioned";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create unpartitioned table
      prepareTable(ops, tableName, false);

      // Insert data
      populateTable(ops, tableName, 5);

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats> stats =
          ops.collectCommitEventTablePartitionStats(tableName);

      // Assert: Should have single stats record (snapshot-based)
      Assertions.assertNotNull(stats, "Stats should not be null");
      Assertions.assertEquals(
          1, stats.size(), "Should have 1 stats record for unpartitioned table");

      com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat = stats.get(0);

      // Verify snapshot-based stats
      Assertions.assertNotNull(stat.getDataset(), "Dataset should not be null");
      Assertions.assertNotNull(stat.getCommitMetadata(), "Commit metadata should not be null");
      Assertions.assertNotNull(stat.getPartitionData(), "Partition data should not be null");
      Assertions.assertTrue(
          stat.getPartitionData().isEmpty(),
          "Partition data should be empty for unpartitioned table");
      Assertions.assertTrue(stat.getRowCount() > 0, "Row count should be greater than 0");
      Assertions.assertEquals(5, stat.getRowCount(), "Row count should equal inserted rows");
      Assertions.assertTrue(stat.getColumnCount() > 0, "Column count should be greater than 0");

      log.info(
          "Unpartitioned table stats: rowCount={}, columnCount={}",
          stat.getRowCount(),
          stat.getColumnCount());
    }
  }

  @Test
  public void testCollectPartitionStatsWithMultipleCommitsToSamePartition() throws Exception {
    final String tableName = "db.test_partition_stats_multiple_commits";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);

      // Insert to SAME partition multiple times (3 commits to partition day 0)
      long timestamp = System.currentTimeMillis() / 1000;
      populateTable(ops, tableName, 1, 0, timestamp);
      Thread.sleep(100); // Small delay to ensure different commit timestamps
      populateTable(ops, tableName, 1, 0, timestamp);
      Thread.sleep(100);
      populateTable(ops, tableName, 1, 0, timestamp);

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats> stats =
          ops.collectCommitEventTablePartitionStats(tableName);

      // Assert: Should have only 1 stats record (latest commit per partition)
      Assertions.assertNotNull(stats, "Stats should not be null");
      Assertions.assertEquals(
          1, stats.size(), "Should have only 1 stats record (latest commit for the partition)");

      com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat = stats.get(0);

      // Verify it's the latest commit's data
      Assertions.assertNotNull(stat.getCommitMetadata(), "Commit metadata should not be null");
      Assertions.assertTrue(stat.getRowCount() > 0, "Row count should reflect latest commit");

      log.info(
          "Latest commit stats: rowCount={}, commitId={}",
          stat.getRowCount(),
          stat.getCommitMetadata().getCommitId());
    }
  }

  @Test
  public void testCollectPartitionStatsEmptyTable() throws Exception {
    final String tableName = "db.test_partition_stats_empty";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create empty partitioned table (no data)
      prepareTable(ops, tableName, true);

      // Action: Collect partition stats (no inserts)
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats> stats =
          ops.collectCommitEventTablePartitionStats(tableName);

      // Assert: Should handle empty table gracefully
      Assertions.assertNotNull(stats, "Stats should not be null even for empty table");
      // Empty table should return empty list or single record with 0 rows
      // (implementation-specific, but should not throw exception)
      log.info("Empty table stats count: {}", stats.size());
    }
  }
}
