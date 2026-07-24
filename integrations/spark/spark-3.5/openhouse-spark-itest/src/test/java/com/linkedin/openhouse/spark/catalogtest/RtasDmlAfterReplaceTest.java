package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Black-box "test the contract" suite: after a table has been replaced via RTAS ({@code CREATE OR
 * REPLACE TABLE ... AS SELECT}), ordinary DML must continue to work and produce correct results.
 *
 * <p>The earlier {@link RtasBehaviorTest} pins the structural outcome of the replace itself
 * (schema, partitioning, identity). These tests go a step further and exercise the write path
 * <em>after</em> the replace: INSERT / DELETE / UPDATE / MERGE / INSERT OVERWRITE and reads, on
 * both copy-on-write and merge-on-read tables. RTAS swaps the entire table body atomically, so a
 * regression that left the post-replace table in a state where subsequent DML mis-behaved (wrong
 * rows, or silently dropping merge-on-read semantics) would be a serious correctness bug; these
 * tests would catch it.
 *
 * <p>Everything is driven through Spark SQL against a real embedded OpenHouse server.
 */
public class RtasDmlAfterReplaceTest extends OpenHouseSparkITest {

  private static final String MOR_PROPS =
      "'format-version'='2', "
          + "'write.format.default'='orc', "
          + "'write.delete.mode'='merge-on-read', "
          + "'write.update.mode'='merge-on-read', "
          + "'write.merge.mode'='merge-on-read'";

  /**
   * Creates a replace-enabled table seeded with (1,'a'),(2,'b'),(3,'c'), then RTAS-replaces its
   * body with (10,'a'),(20,'b'),(30,'c') so every test starts from a table that has already been
   * through a replace. When {@code mergeOnRead} is set, merge-on-read is configured on both the
   * original and the replacement table so post-RTAS DML produces delete files rather than rewriting
   * data.
   */
  private static void seedThenReplace(SparkSession spark, String table, boolean mergeOnRead) {
    String tblProps = mergeOnRead ? " TBLPROPERTIES (" + MOR_PROPS + ")" : "";
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql("CREATE TABLE " + table + " (id int, data string) USING iceberg" + tblProps);
    spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('replace.enabled'='true')");
    spark.sql("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    spark.sql(
        "REPLACE TABLE "
            + table
            + " USING iceberg"
            + tblProps
            + " AS SELECT CAST(id * 10 AS int) AS id, data FROM "
            + table);
  }

  private static List<Row> rowsById(SparkSession spark, String table) {
    return spark.sql("SELECT id, data FROM " + table + " ORDER BY id").collectAsList();
  }

  private static long count(SparkSession spark, String table) {
    return spark.sql("SELECT count(*) FROM " + table).collectAsList().get(0).getLong(0);
  }

  private static List<Row> deleteFiles(SparkSession spark, String table) {
    return spark
        .sql("SELECT file_path, record_count FROM " + table + ".delete_files")
        .collectAsList();
  }

  // ----- copy-on-write DML after RTAS -----

  @Test
  public void testInsertAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.insertCow";
      seedThenReplace(spark, table, false);

      spark.sql("INSERT INTO " + table + " VALUES (40, 'd')");

      List<Row> rows = rowsById(spark, table);
      assertEquals(4, rows.size(), "insert should append one row to the replaced body");
      assertEquals(40, rows.get(3).getInt(0));
      assertEquals("d", rows.get(3).getString(1));

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testDeleteAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.deleteCow";
      seedThenReplace(spark, table, false);

      spark.sql("DELETE FROM " + table + " WHERE id = 20");

      List<Row> rows = rowsById(spark, table);
      assertEquals(2, rows.size(), "delete should remove exactly the matched row");
      assertTrue(
          rows.stream().noneMatch(r -> r.getInt(0) == 20), "row 20 should be gone after delete");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testUpdateAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.updateCow";
      seedThenReplace(spark, table, false);

      spark.sql("UPDATE " + table + " SET data = 'updated' WHERE id = 30");

      String updated =
          spark
              .sql("SELECT data FROM " + table + " WHERE id = 30")
              .collectAsList()
              .get(0)
              .getString(0);
      assertEquals("updated", updated, "update should modify the matched row");
      assertEquals(3, count(spark, table), "update must not change the row count");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testMergeAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.mergeCow";
      seedThenReplace(spark, table, false);

      // source (10 -> update, 99 -> insert)
      spark.sql(
          "MERGE INTO "
              + table
              + " t USING (SELECT * FROM VALUES (10, 'merged'), (99, 'new') AS s(id, data)) s "
              + "ON t.id = s.id "
              + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
              + "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)");

      List<Row> rows = rowsById(spark, table);
      assertEquals(4, rows.size(), "merge should have inserted the unmatched source row");
      assertEquals(
          "merged",
          spark
              .sql("SELECT data FROM " + table + " WHERE id = 10")
              .collectAsList()
              .get(0)
              .getString(0),
          "merge should have updated the matched row");
      assertTrue(
          rows.stream().anyMatch(r -> r.getInt(0) == 99), "merge should have inserted id 99");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testInsertOverwriteAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.overwriteCow";
      seedThenReplace(spark, table, false);

      spark.sql("INSERT OVERWRITE " + table + " VALUES (100, 'only')");

      List<Row> rows = rowsById(spark, table);
      assertEquals(1, rows.size(), "insert overwrite should replace the entire body");
      assertEquals(100, rows.get(0).getInt(0));
      assertEquals("only", rows.get(0).getString(1));

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  @Test
  public void testReadAfterRtas() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.readCow";
      seedThenReplace(spark, table, false);

      List<Row> rows = rowsById(spark, table);
      assertEquals(3, rows.size(), "read should return the replaced body");
      assertEquals(10, rows.get(0).getInt(0));
      assertEquals(20, rows.get(1).getInt(0));
      assertEquals(30, rows.get(2).getInt(0));

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  // ----- merge-on-read DML after RTAS (must still produce delete files) -----

  /**
   * After a merge-on-read table is replaced via RTAS, a DELETE must still take the merge-on-read
   * path and emit position delete files (rather than silently rewriting data files) — i.e. the
   * merge-on-read semantics survive the replace.
   *
   * <p>The outcome is asserted purely from the {@code .delete_files} metadata table. The table body
   * is intentionally NOT read back: applying position deletes on read goes through Iceberg's own
   * (shaded) delete-loader, which collides with the unshaded Parquet/ORC classes on this shared
   * integration-test task's classpath. The dedicated {@code DeleteFileReplicationTestSpark} uses
   * the same metadata-only strategy for the same reason. Copy-on-write UPDATE/MERGE-after-RTAS
   * correctness is covered above; merge-on-read UPDATE/MERGE read the target through that same
   * delete-loader and so cannot be exercised in this task.
   */
  @Test
  public void testDeleteAfterRtasMergeOnRead() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse.dbRtasDml.deleteMor";
      seedThenReplace(spark, table, true);

      spark.sql("DELETE FROM " + table + " WHERE id = 20");

      List<Row> deletes = deleteFiles(spark, table);
      assertFalse(
          deletes.isEmpty(), "merge-on-read delete after RTAS must produce position delete files");
      deletes.forEach(
          r ->
              assertTrue(
                  r.getString(0).endsWith(".orc"),
                  "merge-on-read delete files should be ORC: " + r.getString(0)));
      long deletedRecords = deletes.stream().mapToLong(r -> r.getLong(1)).sum();
      assertEquals(
          1L, deletedRecords, "the position delete files should cover exactly the one deleted row");

      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }
}
