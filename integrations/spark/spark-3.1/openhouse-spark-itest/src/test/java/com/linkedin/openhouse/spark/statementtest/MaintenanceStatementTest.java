package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.spark.maintenance.OrphanTableDirectory;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** VACUUM TABLE and DELETE ORPHAN DIRECTORY against real Iceberg tables in a Hadoop catalog. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MaintenanceStatementTest {
  private SparkSession spark;
  private FileSystem fs;
  private String warehouse;

  @BeforeAll
  public void setupSpark() throws Exception {
    warehouse = Files.createTempDirectory("unittest_maintenance").toRealPath().toString();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.openhouse.type", "hadoop")
            .config("spark.sql.catalog.openhouse.warehouse", warehouse)
            .config(OrphanTableDirectory.ROOT_PATH_CONF, warehouse)
            .getOrCreate();
    fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
  }

  @AfterEach
  public void dropTables() {
    spark.sql("DROP TABLE IF EXISTS openhouse.db.t");
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  @Test
  public void vacuumDryRunChangesNothingAndRealRunFollowsHistoryPolicy() throws Exception {
    createTable("t", "'policies'='{\"history\":{\"versions\":1}}'");
    Path orphan = plantFile(new Path(location("t"), "data/orphan.parquet"), 5);

    Map<String, Long> dry = metrics("VACUUM TABLE openhouse.db.t DRY RUN");
    Assertions.assertEquals(2L, dry.get("expired_snapshots"));
    Assertions.assertEquals(1L, dry.get("orphan_files_deleted"));
    Assertions.assertEquals(3L, snapshots("t"));
    Assertions.assertTrue(fs.exists(orphan));

    Map<String, Long> real = metrics("VACUUM TABLE openhouse.db.t");
    Assertions.assertEquals(2L, real.get("expired_snapshots"));
    Assertions.assertTrue(real.get("expired_files_deleted") > 0);
    Assertions.assertEquals(1L, real.get("orphan_files_deleted"));
    Assertions.assertEquals(0L, real.get("orphan_files_failed"));
    Assertions.assertEquals(1L, snapshots("t"));
    Assertions.assertFalse(fs.exists(orphan));
    Assertions.assertEquals(3L, spark.sql("SELECT * FROM openhouse.db.t").count());
  }

  @Test
  public void vacuumWithoutPolicyKeepsSnapshotsYoungerThanThreeDays() {
    createTable("t", "'openhouse.tableType'='PRIMARY_TABLE'");
    Assertions.assertEquals(0L, metrics("VACUUM TABLE openhouse.db.t").get("expired_snapshots"));
    Assertions.assertEquals(3L, snapshots("t"));
  }

  @Test
  public void vacuumOnReplicaSkipsExpirationAndKeepsThreeDayOrphanMinimum() throws Exception {
    createTable(
        "t",
        "'policies'='{\"history\":{\"versions\":1}}', 'openhouse.tableType'='REPLICA_TABLE', "
            + "'ofd.one_day_ttl.enabled'='true'");
    Path young = plantFile(new Path(location("t"), "data/young.parquet"), 2);
    Path old = plantFile(new Path(location("t"), "data/old.parquet"), 4);

    Map<String, Long> result = metrics("VACUUM TABLE openhouse.db.t");
    Assertions.assertFalse(result.containsKey("expired_snapshots"));
    Assertions.assertEquals(3L, snapshots("t"));
    Assertions.assertTrue(fs.exists(young));
    Assertions.assertFalse(fs.exists(old));
  }

  @Test
  public void vacuumKeepsBackupFilesAndMetadata() throws Exception {
    createTable("t", "'policies'='{\"history\":{\"versions\":1}}'");
    Path backup = plantFile(new Path(location("t"), ".backup/data/kept.parquet"), 10);
    Path metadata = plantFile(new Path(location("t"), "metadata/stray.metadata.json"), 10);

    metrics("VACUUM TABLE openhouse.db.t");
    Assertions.assertTrue(fs.exists(backup));
    Assertions.assertTrue(fs.exists(metadata));
  }

  @Test
  public void vacuumRefusesNonOpenhouseTable() {
    spark.sql("CREATE TABLE openhouse.db.t (id bigint) USING iceberg");
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> spark.sql("VACUUM TABLE openhouse.db.t"));
  }

  @Test
  public void deleteOrphanDirectoryEmptiesADroppedTablesDirectory() throws Exception {
    String orphan = orphanedCopy("gone");

    List<Row> dry = spark.sql("DELETE ORPHAN DIRECTORY '" + orphan + "' DRY RUN").collectAsList();
    Assertions.assertEquals(
        "metadata", new Path(dry.get(dry.size() - 1).getString(0)).getName(), "metadata last");
    Assertions.assertTrue(dry.stream().allMatch(r -> r.getString(2).equals("would delete")));
    Assertions.assertEquals(dry.size(), fs.listStatus(new Path(orphan)).length);

    List<Row> real = spark.sql("DELETE ORPHAN DIRECTORY '" + orphan + "'").collectAsList();
    Assertions.assertEquals(dry.size(), real.size());
    Assertions.assertTrue(fs.exists(new Path(orphan)));
    Assertions.assertEquals(0, fs.listStatus(new Path(orphan)).length);
  }

  @Test
  public void deleteOrphanDirectoryRefusesALiveTable() throws Exception {
    createTable("t", "'openhouse.databaseId'='db'");
    String live = rel(location("t"));
    age(new Path(live), 10);
    assertRefused(live, "live table openhouse.db.t");
    Assertions.assertEquals(3L, spark.sql("SELECT * FROM openhouse.db.t").count());
  }

  @Test
  public void deleteOrphanDirectoryRefusesRecentDirectories() throws Exception {
    String orphan = orphanedCopy("recent");
    plantFile(new Path(orphan, "data/new.parquet"), 0);
    age(new Path(orphan), 0);
    assertRefused(orphan, "no file older than 7 days");
  }

  @Test
  public void deleteOrphanDirectoryRefusesUnreadableMetadata() throws Exception {
    String orphan = orphanedCopy("corrupt");
    Path junk = new Path(orphan, "metadata/v99.metadata.json");
    try (FSDataOutputStream out = fs.create(junk)) {
      out.writeBytes("{not json");
    }
    age(new Path(orphan), 10);
    age(junk, 9);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> spark.sql("DELETE ORPHAN DIRECTORY '" + orphan + "' DRY RUN"));
    Assertions.assertTrue(fs.exists(junk));
  }

  @Test
  public void deleteOrphanDirectoryRefusesPathsOutsideTableDirectories() {
    String expected = "is not " + warehouse + "/<database>/<directory>";
    assertRefused(warehouse + "/db", expected);
    assertRefused(warehouse + "/db/x/data", expected);
    assertRefused(warehouse + "/db/../db", expected);
    assertRefused("file:" + warehouse + "/db/x", expected);
    assertRefused("/elsewhere/db/x", expected);
  }

  private void assertRefused(String path, String message) {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> spark.sql("DELETE ORPHAN DIRECTORY '" + path + "' DRY RUN"));
    Assertions.assertTrue(e.getMessage().contains(message), e.getMessage());
  }

  /** A copy of a dropped table's directory, aged past the orphan threshold. */
  private String orphanedCopy(String name) throws Exception {
    createTable(name, "'openhouse.databaseId'='db'");
    Path copy = new Path(warehouse, "db/" + name + "-orphan");
    FileUtil.copy(fs, new Path(location(name)), fs, copy, false, fs.getConf());
    spark.sql("DROP TABLE openhouse.db." + name);
    age(copy, 10);
    return copy.toString();
  }

  private void createTable(String name, String properties) {
    spark.sql(
        String.format(
            "CREATE TABLE openhouse.db.%s (id bigint) USING iceberg "
                + "TBLPROPERTIES ('openhouse.tableId'='%s', %s)",
            name, name, properties));
    for (int i = 0; i < 3; i++) {
      spark.sql(String.format("INSERT INTO openhouse.db.%s VALUES (%d)", name, i));
    }
  }

  private Map<String, Long> metrics(String sql) {
    return spark.sql(sql).collectAsList().stream()
        .collect(Collectors.toMap(r -> r.getString(0), r -> r.getLong(1)));
  }

  private long snapshots(String name) {
    return spark.sql("SELECT * FROM openhouse.db." + name + ".snapshots").count();
  }

  private String location(String name) throws Exception {
    return Spark3Util.loadIcebergTable(spark, "openhouse.db." + name).location();
  }

  private static String rel(String location) {
    return Path.getPathWithoutSchemeAndAuthority(new Path(location)).toString();
  }

  private Path plantFile(Path path, int daysOld) throws Exception {
    fs.create(path).close();
    fs.setTimes(path, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(daysOld), -1);
    return path;
  }

  private void age(Path path, int daysOld) throws Exception {
    fs.setTimes(path, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(daysOld), -1);
    if (fs.getFileStatus(path).isDirectory()) {
      for (FileStatus child : fs.listStatus(path)) {
        age(child.getPath(), daysOld);
      }
    }
  }
}
