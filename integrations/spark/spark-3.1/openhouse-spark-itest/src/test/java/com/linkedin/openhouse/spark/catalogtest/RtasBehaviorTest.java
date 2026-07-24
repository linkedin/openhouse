package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Black-box behavior tests for RTAS ({@code CREATE OR REPLACE TABLE ... AS SELECT}) driven entirely
 * through Spark SQL against a real embedded OpenHouse server. RTAS defines a brand new table body,
 * so it is intentionally allowed schema/partition evolutions that the incremental {@code ALTER
 * TABLE} path forbids, while still preserving the table's identity. These tests pin that contract
 * so a future change can't silently start applying the update-path guards to the replace path (or
 * lose the table identity across a replace).
 */
public class RtasBehaviorTest extends OpenHouseSparkITest {

  private static void createSeededReplaceEnabledTable(
      SparkSession spark, String table, String schema, String partitionClause) {
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql("CREATE TABLE " + table + " " + schema + " USING iceberg " + partitionClause);
    spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
  }

  private static List<String> columnsOf(SparkSession spark, String table) {
    return spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }

  private static Map<String, String> tableProperties(SparkSession spark, String table) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
  }

  private static List<String> dataFilePaths(SparkSession spark, String table) {
    return spark.sql("SELECT file_path FROM " + table + ".files").collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }

  /**
   * Strips the leading {@code openhouse.} catalog prefix, leaving the {@code db.table} identifier.
   */
  private static String tableArg(String fullyQualifiedTable) {
    return fullyQualifiedTable.substring(fullyQualifiedTable.indexOf('.') + 1);
  }

  @Test
  public void testRtasMayDropColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.dropColumn";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string, keep string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a', 'x'), (2, 'b', 'y')");

      // Dropping a column is forbidden by ALTER TABLE but allowed by RTAS.
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, keep FROM " + table);

      List<String> columns = columnsOf(spark, table);
      assertTrue(columns.contains("id") && columns.contains("keep"), "kept columns should remain");
      assertFalse(columns.contains("data"), "RTAS should have dropped the 'data' column");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayAddColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.addColumn";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')");

      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg AS SELECT id, data, CAST(id * 10 AS bigint) AS derived FROM "
              + table);

      assertTrue(columnsOf(spark, table).contains("derived"), "RTAS should have added 'derived'");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayChangePartitionSpec() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.changeSpec";
      createSeededReplaceEnabledTable(
          spark, table, "(id bigint, part string)", "PARTITIONED BY (id)");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'x'), (2, 'y')");

      // Repartition by a different column — forbidden by ALTER, allowed by RTAS.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg PARTITIONED BY (part) AS SELECT id, part FROM "
              + table);

      List<Row> parts = spark.sql("SELECT * FROM " + table + ".partitions").collectAsList();
      assertEquals(2, parts.size(), "expected two partitions after re-partitioning by part");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayRemovePartitioning() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.removePartitioning";
      createSeededReplaceEnabledTable(
          spark, table, "(id bigint, part string)", "PARTITIONED BY (part)");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'x'), (2, 'y')");

      // Replace with an unpartitioned table body.
      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, part FROM " + table);

      List<Row> specFields = spark.sql("SELECT * FROM " + table + ".partitions").collectAsList();
      // An unpartitioned Iceberg table reports a single (empty) partition row.
      assertEquals(1, specFields.size(), "expected an unpartitioned table after RTAS");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasReplacesData() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.replacesData";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

      // Replace the body with a filtered projection; the row set must reflect the SELECT.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg AS SELECT id, data FROM "
              + table
              + " WHERE id <= 2");

      long count = spark.sql("SELECT count(*) FROM " + table).collectAsList().get(0).getLong(0);
      assertEquals(2L, count, "RTAS should have replaced the data with the filtered row set");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasPreservesTableIdentity() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.identity";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");

      String uuidBefore = tableProperties(spark, table).get("openhouse.tableUUID");
      assertNotNull(uuidBefore, "table should have an openhouse.tableUUID before RTAS");

      spark.sql("REPLACE TABLE " + table + " USING iceberg AS SELECT id, data FROM " + table);

      String uuidAfter = tableProperties(spark, table).get("openhouse.tableUUID");
      assertEquals(
          uuidBefore, uuidAfter, "RTAS must preserve the table identity (openhouse.tableUUID)");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasRejectedWhenReplaceNotEnabled() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.replaceDisabled";
      spark.sql("DROP TABLE IF EXISTS " + table);
      // Create WITHOUT opting into RTAS.
      spark.sql("CREATE TABLE " + table + " (id bigint, data string) USING iceberg");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");

      Exception e =
          org.junit.jupiter.api.Assertions.assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                      "REPLACE TABLE "
                          + table
                          + " USING iceberg AS SELECT id, data FROM "
                          + table));
      assertTrue(
          e.getMessage() != null
              && e.getMessage().contains("REPLACE TABLE AS SELECT is not enabled"),
          "expected an RTAS-disabled error but got: " + e.getMessage());

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayChangeFileFormatOrcToParquet() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.formatChange";
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string) USING iceberg TBLPROPERTIES ('write.format.default'='orc')");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");
      assertTrue(
          dataFilePaths(spark, table).stream().allMatch(p -> p.endsWith(".orc")),
          "seed data files should be ORC");

      // Changing the file format is a valid RTAS transform: re-specify parquet as the write
      // default.
      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg TBLPROPERTIES ('write.format.default'='parquet')"
              + " AS SELECT id, data FROM "
              + table);

      assertEquals(
          "parquet",
          tableProperties(spark, table).get("write.format.default"),
          "RTAS should have switched the default write format to parquet");
      List<String> files = dataFilePaths(spark, table);
      assertFalse(files.isEmpty(), "table should have data files after RTAS");
      assertTrue(
          files.stream().allMatch(p -> p.endsWith(".parquet")),
          "all data files after RTAS should be parquet, got: " + files);

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasMayRemoveEncryption() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.removeEncryption";
      spark.sql("DROP TABLE IF EXISTS " + table);
      // OpenHouse OSS has no KMS-backed encryption; the real encrypted->unencrypted transform is
      // exercised in li-openhouse. Here we pin that RTAS *permits* the transform and lands the
      // table
      // in the requested (unencrypted) state, using a table property to stand in for the setting.
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id bigint, data string) USING iceberg TBLPROPERTIES ('encryption.enabled'='true')");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a')");
      assertEquals(
          "true",
          tableProperties(spark, table).get("encryption.enabled"),
          "table should start with encryption enabled");

      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg TBLPROPERTIES ('encryption.enabled'='false')"
              + " AS SELECT id, data FROM "
              + table);

      assertEquals(
          "false",
          tableProperties(spark, table).get("encryption.enabled"),
          "RTAS should be allowed to turn encryption off");
      assertEquals(
          1L,
          spark.sql("SELECT count(*) FROM " + table).collectAsList().get(0).getLong(0),
          "table must remain readable after the encryption-removing RTAS");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasLeavesPreReplaceSnapshotReachableOnDisconnectedTimeline() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.timeTravel";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string, extra string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a', 'keep')");
      long preReplaceSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + table + ".snapshots")
              .collectAsList()
              .get(0)
              .getLong(0);

      // RTAS drops the 'extra' column and rewrites the row body.
      spark.sql(
          "REPLACE TABLE " + table + " USING iceberg AS SELECT id, 'b' AS data FROM " + table);
      assertFalse(columnsOf(spark, table).contains("extra"), "RTAS should have dropped 'extra'");

      // The pre-replace snapshot is retained and still reachable by explicit time travel, but it
      // sits on a disconnected timeline: it carries the OLD schema and the OLD row body. The spec's
      // intended end-state is for cross-boundary time travel to error; today it silently returns
      // the
      // pre-replace data, which this test pins so a future gate change is caught here.
      List<Row> old =
          spark
              .sql("SELECT * FROM " + table + " VERSION AS OF " + preReplaceSnapshot)
              .collectAsList();
      assertEquals(1, old.size(), "pre-replace snapshot should still be readable");
      assertEquals(
          3, old.get(0).length(), "pre-replace snapshot should carry the old 3-column schema");
      assertEquals(
          "keep", old.get(0).getString(2), "pre-replace snapshot should return the old body");

      assertEquals(
          "b",
          spark.sql("SELECT data FROM " + table).collectAsList().get(0).getString(0),
          "current body should be the replaced one");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testRtasAllowsRestoringPreReplaceSnapshotWithDataLoss() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasBehavior.restore";
      createSeededReplaceEnabledTable(spark, table, "(id bigint, data string)", "");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')");
      long preReplaceSnapshot =
          spark
              .sql("SELECT snapshot_id FROM " + table + ".snapshots")
              .collectAsList()
              .get(0)
              .getLong(0);

      spark.sql(
          "REPLACE TABLE "
              + table
              + " USING iceberg AS SELECT CAST(id * 100 AS bigint) AS id, data FROM "
              + table);
      assertEquals(
          1,
          spark.sql("SELECT id FROM " + table + " WHERE id = 100").collectAsList().size(),
          "sanity: replaced body should contain id 100");

      // Restore the table to its pre-replace snapshot. Per the RTAS spec this is supported, at the
      // cost of losing the snapshots created after it (the replaced body).
      spark.sql(
          "CALL openhouse.system.set_current_snapshot(table => '"
              + tableArg(table)
              + "', snapshot_id => "
              + preReplaceSnapshot
              + ")");

      List<Row> restored =
          spark.sql("SELECT id, data FROM " + table + " ORDER BY id").collectAsList();
      assertEquals(2, restored.size(), "restore should bring back the pre-replace row count");
      assertEquals(
          1L, restored.get(0).getLong(0), "restore should bring back the pre-replace body");
      assertEquals(
          2L, restored.get(1).getLong(0), "restore should bring back the pre-replace body");
      assertTrue(
          spark.sql("SELECT id FROM " + table + " WHERE id = 100").collectAsList().isEmpty(),
          "the replaced body should be lost after restoring the earlier snapshot");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }
}
