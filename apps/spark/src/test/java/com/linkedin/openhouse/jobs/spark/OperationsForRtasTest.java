package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Coverage for maintenance jobs running against a table that has been replaced by {@code REPLACE
 * TABLE ... AS SELECT}.
 *
 * <p>Retention is the job that a replace can actually invalidate: the scheduler snapshots {@code
 * policies.retention} and freezes it into the job's CLI args, and {@link Operations#runRetention}
 * never re-reads the policy before issuing its DELETE. Two tests pin the ways that goes wrong. The
 * remaining two cover snapshot expiration and orphan file deletion, which carry no such frozen
 * state and are expected to simply work on a replaced table.
 */
@Slf4j
public class OperationsForRtasTest extends OpenHouseSparkITest {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final String BACKUP_DIR = ".backup";
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  /**
   * Mismatch between the old retention config and the current schema.
   *
   * <p>The job was launched with {@code --columnName datepartition}. The replace drops that column,
   * so the frozen args describe a schema the table no longer has. Nothing revalidates them, so the
   * mismatch only surfaces when the generated DELETE fails to resolve.
   *
   * <p>Note the replace is only accepted because the policy was re-pointed at a surviving column
   * first: OpenHouse rejects a replace that would leave {@code policies.retention} referencing a
   * dropped column. It does not extend that protection to a job already holding the old config.
   */
  @Test
  public void testRetentionConfigMismatchesSchemaAfterRtas() throws Exception {
    final String tableName = "db.test_retention_config_schema_mismatch";
    final String sourceName = "db.test_retention_config_schema_mismatch_source";
    ZonedDateTime now = ZonedDateTime.now();
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Table retains on `datepartition`. Neither retention column is a partition field: a replace
      // cannot drop a column the retained partition spec still binds to.
      ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, datepartition string, newdate string, part string)"
                      + " PARTITIONED BY (part)",
                  tableName))
          .show();
      ops.spark()
          .sql(
              String.format(
                  "ALTER TABLE %s SET POLICY (RETENTION = 30d ON COLUMN datepartition"
                      + " WHERE PATTERN = 'yyyy-MM-dd')",
                  tableName));
      ops.spark()
          .sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('replace.enabled'='true')", tableName))
          .show();
      String today = DATE_FORMATTER.format(now);
      String longAgo = DATE_FORMATTER.format(now.minusDays(40));
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES ('d0', '%s', '%s', 'p0'), ('d40', '%s', '%s', 'p1')",
                  tableName, today, today, longAgo, longAgo))
          .show();
      verifyRowCount(ops, tableName, 2);

      // The scheduler snapshots the policy here and launches the job, freezing
      // `--columnName datepartition --columnPattern yyyy-MM-dd --granularity day --count 30`.

      // The owner migrates the retention column and replaces the table, dropping the old column.
      ops.spark()
          .sql(
              String.format(
                  "ALTER TABLE %s SET POLICY (RETENTION = 30d ON COLUMN newdate"
                      + " WHERE PATTERN = 'yyyy-MM-dd')",
                  tableName));
      ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", sourceName)).show();
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, newdate string, part string)", sourceName))
          .show();
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES ('d0', '%s', 'p0'), ('d40', '%s', 'p1')",
                  sourceName, today, longAgo))
          .show();
      ops.spark()
          .sql(
              String.format(
                  "REPLACE TABLE %s USING iceberg PARTITIONED BY (part)"
                      + " AS SELECT data, newdate, part FROM %s",
                  tableName, sourceName));
      Table replaced = ops.getTable(tableName);
      Assertions.assertNull(
          replaced.schema().findField("datepartition"),
          "Replace should have dropped the column the in-flight job was launched with");
      Assertions.assertNotNull(
          replaced.schema().findField("newdate"), "Replace should keep the new retention column");
      verifyRowCount(ops, tableName, 2);

      // The in-flight job runs with its frozen args, which no longer match the schema.
      AnalysisException e =
          Assertions.assertThrows(
              AnalysisException.class,
              () ->
                  ops.runRetention(
                      tableName, "datepartition", "yyyy-MM-dd", "day", 30, false, "", now),
              "Retention should not resolve against a schema missing its configured column");
      Assertions.assertTrue(
          e.getMessage().contains("datepartition"),
          "Failure should name the stale retention column, but was: " + e.getMessage());

      // The mismatch is fatal rather than destructive: the replaced table is untouched.
      verifyRowCount(ops, tableName, 2);
    }
  }

  /**
   * The retention DELETE runs against a table replaced after the job's args were frozen.
   *
   * <p>The scheduler snapshots {@code policies.retention} and freezes it into the job's args. The
   * replace lands before the job issues its DELETE, changing what the table holds. Spark plans and
   * commits {@code DELETE FROM} inside a single statement, so there is no window for the replace to
   * invalidate: by the time the write is validated, the scan behind it has already seen the
   * replacement. Iceberg's conflict validations are bound to that scan, so nothing rejects a
   * retention window that was authorized against a table which no longer exists. The frozen window
   * is applied to the replacement's rows instead.
   *
   * <p>A preflight check at job start cannot cover this, since it would have passed before the
   * replace landed. Closing it needs the window bound to the state that was validated.
   */
  @Test
  public void testRetentionDeleteOnSnapshotStaledByRtas() throws Exception {
    final String tableName = "db.test_retention_delete_on_staled_snapshot";
    final String sourceName = "db.test_retention_delete_on_staled_snapshot_source";
    ZonedDateTime now = ZonedDateTime.now();
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareRetentionTable(ops, tableName, "1d");
      insertRows(ops, tableName, now, 0, 2);
      verifyRowCount(ops, tableName, 2);

      // The scheduler snapshots the policy here and launches the job, freezing
      // `--columnName ts --granularity day --count 1`. The job plans against this snapshot.
      long preRtasSnapshotId = ops.getTable(tableName).currentSnapshot().snapshotId();

      // The replace lands first and stales that snapshot. Its d5 row sits outside the window the
      // job is holding, and was never covered by the policy the job was launched with.
      prepareSource(ops, sourceName, now, 0, 5);
      ops.spark()
          .sql(
              String.format(
                  "REPLACE TABLE %s USING iceberg PARTITIONED BY (days(ts))"
                      + " AS SELECT data, ts FROM %s",
                  tableName, sourceName));
      long rtasSnapshotId = ops.getTable(tableName).currentSnapshot().snapshotId();
      Assertions.assertNotEquals(
          preRtasSnapshotId,
          rtasSnapshotId,
          "Replace should have moved the table off the snapshot the job planned on");
      verifyRowCount(ops, tableName, 2);

      // The job issues its DELETE with the frozen args. A metadata-only delete carries no snapshot
      // validation, so the replace does not invalidate it.
      Assertions.assertDoesNotThrow(
          () -> ops.runRetention(tableName, "ts", "", "day", 1, false, "", now),
          "Current behavior: Spark's DELETE FROM is not validated against the replace");

      // It commits on top of the replacement. Spark 3.1 / Iceberg 1.2 rewrites data files
      // (`overwrite`, i.e. OverwriteFiles, the path that does expose validateFromSnapshot and
      // validateNoConflictingData); Spark 3.5 / Iceberg 1.5 resolves the same predicate to a
      // metadata-only `delete`. Neither rejects the write: the validations that exist are bound to
      // the scan Spark just planned, which already saw the replacement, so no conflict is left to
      // detect.
      Table afterDelete = ops.getTable(tableName);
      Assertions.assertNotEquals(
          rtasSnapshotId,
          afterDelete.currentSnapshot().snapshotId(),
          "Retention delete should have committed a snapshot");
      Assertions.assertTrue(
          Arrays.asList("delete", "overwrite").contains(afterDelete.currentSnapshot().operation()),
          "Retention should have committed a write on top of the replacement, but was: "
              + afterDelete.currentSnapshot().operation());
      ops.spark().sql(String.format("REFRESH TABLE %s", tableName));
      verifyRowCount(ops, tableName, 1);
      Assertions.assertEquals(
          0,
          ops.spark().sql(String.format("SELECT * FROM %s WHERE data = 'd5'", tableName)).count(),
          "The replacement's out-of-window row is deleted by the stale config");
      Assertions.assertEquals(
          0,
          rowsOlderThan(ops, tableName, now.minusDays(1)),
          "Retention applied the frozen window to data it was never validated against");
    }
  }

  /**
   * Snapshot expiration after a replace.
   *
   * <p>RTAS resets the branch to a single new snapshot but leaves the pre-RTAS snapshots in the
   * table's snapshot list, where they keep the replaced data files alive. SE is what actually
   * prunes them, so this checks it still runs against a replaced table and leaves the replacement's
   * snapshot as the only survivor.
   */
  @Test
  public void testSnapshotExpirationAfterRtas() throws Exception {
    final String tableName = "db.test_se_after_rtas";
    final String sourceName = "db.test_se_after_rtas_source";
    ZonedDateTime now = ZonedDateTime.now();
    long rtasSnapshotId;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareReplaceableTable(ops, tableName);
      insertRows(ops, tableName, now, 0, 1, 2);
      List<Long> preRtasSnapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(3, preRtasSnapshotIds.size(), "One snapshot per insert");

      prepareSource(ops, sourceName, now, 3, 4);
      ops.spark()
          .sql(
              String.format(
                  "REPLACE TABLE %s USING iceberg AS SELECT data, ts FROM %s",
                  tableName, sourceName));

      // The replace adds a snapshot and points the branch at it, but retains the old ones.
      rtasSnapshotId = ops.getTable(tableName).currentSnapshot().snapshotId();
      List<Long> afterRtasSnapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertTrue(
          afterRtasSnapshotIds.containsAll(preRtasSnapshotIds),
          "Replace should retain the pre-RTAS snapshots, not drop them");
      Assertions.assertEquals(
          preRtasSnapshotIds.size() + 1,
          afterRtasSnapshotIds.size(),
          "Replace should have added exactly one snapshot");

      Table table = ops.getTable(tableName);
      ops.expireSnapshots(table, 0, "DAYS", 0);
      checkSnapshots(table, Collections.singletonList(rtasSnapshotId));
      for (long preRtasSnapshotId : preRtasSnapshotIds) {
        Assertions.assertNull(
            table.snapshot(preRtasSnapshotId),
            "Pre-RTAS snapshot " + preRtasSnapshotId + " should have been expired");
      }
    }
    // Restart the app so the assertions below go through a fresh catalog load.
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      Assertions.assertEquals(
          Collections.singletonList(rtasSnapshotId),
          getSnapshotIds(ops, tableName),
          "Expiration should have persisted");
      verifyRowCount(ops, tableName, 2);
      Assertions.assertEquals(
          2,
          ops.spark()
              .sql(String.format("SELECT * FROM %s WHERE data IN ('d3', 'd4')", tableName))
              .count(),
          "The replacement's rows should still be readable after expiration");
    }
  }

  /**
   * Orphan file deletion after a replace.
   *
   * <p>The replaced data files only become unreferenced once SE has expired the snapshots that
   * still point at them, and SE runs with {@code cleanExpiredFiles(false)}, so it leaves them on
   * disk. OFD is what reclaims them. This walks that chain and checks OFD does not touch the
   * replacement's own files at any point along it.
   */
  @Test
  public void testOrphanFileDeletionAfterRtas() throws Exception {
    final String tableName = "db.test_ofd_after_rtas";
    final String sourceName = "db.test_ofd_after_rtas_source";
    final String plantedOrphanName = "data/test_orphan_file.orc";
    ZonedDateTime now = ZonedDateTime.now();
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareReplaceableTable(ops, tableName);
      insertRows(ops, tableName, now, 0, 1, 2);
      List<String> preRtasDataFiles = getDataFilePaths(ops, tableName);
      Assertions.assertEquals(3, preRtasDataFiles.size(), "One data file per insert");

      prepareSource(ops, sourceName, now, 3, 4);
      ops.spark()
          .sql(
              String.format(
                  "REPLACE TABLE %s USING iceberg AS SELECT data, ts FROM %s",
                  tableName, sourceName));
      List<String> postRtasDataFiles = getDataFilePaths(ops, tableName);
      Assertions.assertFalse(
          postRtasDataFiles.isEmpty(), "Replacement should have written its own data files");
      Assertions.assertTrue(
          Collections.disjoint(preRtasDataFiles, postRtasDataFiles),
          "Replacement should not reuse the pre-RTAS data files");

      Table table = ops.getTable(tableName);
      FileSystem fs = ops.fs();
      Path plantedOrphan = new Path(table.location(), plantedOrphanName);
      fs.createNewFile(plantedOrphan);
      assertFilesExist(fs, preRtasDataFiles, true, "Replaced files should still be on disk");

      // First pass, before expiration. The replaced files are still reachable from the retained
      // snapshots, so OFD must leave them alone and only take the planted orphan.
      DeleteOrphanFiles.Result beforeExpiry =
          ops.deleteOrphanFiles(
              table, System.currentTimeMillis(), false, BACKUP_DIR, 5, false, 20000);
      List<String> orphansBeforeExpiry = Lists.newArrayList(beforeExpiry.orphanFileLocations());
      Assertions.assertTrue(
          orphansBeforeExpiry.stream().anyMatch(f -> f.endsWith(plantedOrphanName)),
          "Planted orphan should be detected: " + orphansBeforeExpiry);
      Assertions.assertFalse(fs.exists(plantedOrphan), "Planted orphan should be removed");
      assertFilesExist(
          fs, preRtasDataFiles, true, "Files reachable from retained snapshots are not orphans");
      assertFilesExist(fs, postRtasDataFiles, true, "Live files must survive OFD");

      // Expiring the pre-RTAS snapshots is what orphans the replaced files. SE runs with
      // cleanExpiredFiles(false), so they stay on disk for OFD to pick up.
      ops.expireSnapshots(table, 0, "DAYS", 0);
      assertFilesExist(fs, preRtasDataFiles, true, "SE should not delete files itself");

      // Second pass. The replaced files are now unreachable and get reclaimed.
      Table expired = ops.getTable(tableName);
      DeleteOrphanFiles.Result afterExpiry =
          ops.deleteOrphanFiles(
              expired, System.currentTimeMillis(), false, BACKUP_DIR, 5, false, 20000);
      List<String> orphansAfterExpiry = Lists.newArrayList(afterExpiry.orphanFileLocations());
      Assertions.assertTrue(
          normalizePaths(orphansAfterExpiry).containsAll(normalizePaths(preRtasDataFiles)),
          "Replaced data files should be reported as orphans: " + orphansAfterExpiry);
      assertFilesExist(fs, preRtasDataFiles, false, "Replaced data files should be reclaimed");
      assertFilesExist(fs, postRtasDataFiles, true, "Live files must survive OFD");

      verifyRowCount(ops, tableName, 2);
      Assertions.assertEquals(
          2,
          ops.spark()
              .sql(String.format("SELECT * FROM %s WHERE data IN ('d3', 'd4')", tableName))
              .count(),
          "The replacement should still be readable after its predecessor is reclaimed");
    }
  }

  private static void prepareRetentionTable(Operations ops, String tableName, String retention) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string, ts timestamp) PARTITIONED BY (days(ts))", tableName))
        .show();
    ops.spark()
        .sql(String.format("ALTER TABLE %s SET POLICY (RETENTION=%s)", tableName, retention));
    // RTAS is disabled by default; opt the table in before replacing it.
    ops.spark()
        .sql(
            String.format("ALTER TABLE %s SET TBLPROPERTIES ('replace.enabled'='true')", tableName))
        .show();
  }

  private static void prepareSource(
      Operations ops, String sourceName, ZonedDateTime now, int... dayLags) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", sourceName)).show();
    ops.spark()
        .sql(String.format("CREATE TABLE %s (data string, ts timestamp)", sourceName))
        .show();
    insertRows(ops, sourceName, now, dayLags);
  }

  private static void insertRows(
      Operations ops, String tableName, ZonedDateTime now, int... dayLags) {
    for (int dayLag : dayLags) {
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES ('d%d', cast('%s' as timestamp))",
                  tableName, dayLag, DATE_FORMATTER.format(now.minusDays(dayLag))))
          .show();
    }
  }

  private static void verifyRowCount(Operations ops, String tableName, int expectedRowCount) {
    List<Row> resultRows =
        ops.spark().sql(String.format("SELECT * FROM %s", tableName)).collectAsList();
    Assertions.assertEquals(expectedRowCount, resultRows.size());
  }

  private static void prepareReplaceableTable(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark().sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName)).show();
    // RTAS is disabled by default; opt the table in before replacing it.
    ops.spark()
        .sql(
            String.format("ALTER TABLE %s SET TBLPROPERTIES ('replace.enabled'='true')", tableName))
        .show();
  }

  private static List<Long> getSnapshotIds(Operations ops, String tableName) {
    return ops.spark().sql(String.format("SELECT * FROM %s.snapshots", tableName)).collectAsList()
        .stream()
        .map(r -> r.getLong(r.fieldIndex("snapshot_id")))
        .collect(Collectors.toList());
  }

  private static List<String> getDataFilePaths(Operations ops, String tableName) {
    return ops.spark().sql(String.format("SELECT file_path FROM %s.files", tableName))
        .collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }

  private static void checkSnapshots(Table table, List<Long> expectedSnapshotIds) {
    List<Long> foundSnapshotIds =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(Snapshot::snapshotId)
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedSnapshotIds, foundSnapshotIds, "Incorrect list of snapshots");
  }

  /** Orphan locations carry a scheme, the {@code files} metadata table does not. */
  private static List<String> normalizePaths(List<String> paths) {
    return paths.stream().map(p -> new Path(p).toUri().getPath()).collect(Collectors.toList());
  }

  private static void assertFilesExist(
      FileSystem fs, List<String> paths, boolean expected, String message) throws Exception {
    for (String path : paths) {
      Assertions.assertEquals(expected, fs.exists(new Path(path)), message + ": " + path);
    }
  }

  private static long rowsOlderThan(Operations ops, String tableName, ZonedDateTime cutoff) {
    return ops.spark()
        .sql(
            String.format(
                "SELECT * FROM %s WHERE ts < cast('%s' as timestamp)",
                tableName, DATE_FORMATTER.format(cutoff)))
        .count();
  }
}
