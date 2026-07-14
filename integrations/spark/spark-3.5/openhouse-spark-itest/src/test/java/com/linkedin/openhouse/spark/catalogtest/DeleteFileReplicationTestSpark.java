package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the delete file replication factor configuration introduced in
 * com.linkedin.iceberg 1.5.2.17 (linkedin/iceberg#219, #229, #254).
 *
 * <p>These tests drive the full OpenHouse stack through SQL: the table property and session conf
 * must round-trip through the OpenHouse tables service, and merge-on-read DELETE on an ORC table
 * must produce ORC position delete files through the replication-aware write path. The numeric
 * replication factor itself is asserted in linkedin/iceberg's own tests; local filesystem storage
 * ignores replication, so here we verify the configuration is accepted end-to-end and the write
 * path it activates works against the OpenHouse catalog.
 *
 * <p>Verification is metadata-based (the delete_files table reads manifests only): this module's
 * test classpath carries both the shaded spark runtime and unshaded iceberg from the test fixtures,
 * so scans that load ORC position deletes hit a shaded/unshaded TypeDescription ClassCastException.
 * Read-side application of the deletes is covered by linkedin/iceberg's own Spark integration
 * tests.
 */
public class DeleteFileReplicationTestSpark extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_delete_repl";

  @Test
  public void testDeleteFileReplicationFromTableProperty() throws Exception {
    String tableName = "openhouse." + DATABASE + ".orc_tbl_prop";
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE "
              + tableName
              + " (id int, data string) USING iceberg "
              + "TBLPROPERTIES ("
              + "'format-version'='2', "
              + "'write.format.default'='orc', "
              + "'write.delete.mode'='merge-on-read', "
              + "'write.delete-file-replication'='5')");

      // the property must survive the OpenHouse tables service round trip
      Map<String, String> props = tableProperties(spark, tableName);
      assertEquals("5", props.get("write.delete-file-replication"));
      assertEquals("merge-on-read", props.get("write.delete.mode"));

      spark.sql(
          "INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
      spark.sql("DELETE FROM " + tableName + " WHERE id IN (1, 2)");

      // merge-on-read produced ORC position delete files through the replication-aware path
      List<Row> deleteFiles =
          spark
              .sql("SELECT file_path, record_count FROM " + tableName + ".delete_files")
              .collectAsList();
      assertFalse(deleteFiles.isEmpty(), "DELETE should produce position delete files");
      long deletedRecords = 0;
      for (Row deleteFile : deleteFiles) {
        assertTrue(
            deleteFile.getString(0).endsWith(".orc"),
            "Delete file should be ORC: " + deleteFile.getString(0));
        deletedRecords += deleteFile.getLong(1);
      }
      assertEquals(2, deletedRecords, "Position delete files should cover the two deleted rows");

      spark.sql("DROP TABLE " + tableName);
    }
  }

  @Test
  public void testDeleteFileReplicationFromSessionConf() throws Exception {
    String tableName = "openhouse." + DATABASE + ".orc_tbl_conf";
    String confKey = "spark.sql.iceberg.delete-file-replication";
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE "
              + tableName
              + " (id int, data string) USING iceberg "
              + "TBLPROPERTIES ("
              + "'format-version'='2', "
              + "'write.format.default'='orc', "
              + "'write.delete.mode'='merge-on-read')");

      spark.sql(
          "INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

      // the key contains hyphens, so the Spark SQL parser requires backquotes
      spark.sql("SET `" + confKey + "`=7");
      try {
        spark.sql("DELETE FROM " + tableName + " WHERE id IN (1, 2)");
      } finally {
        spark.conf().unset(confKey);
      }

      List<Row> deleteFiles =
          spark
              .sql("SELECT file_path, record_count FROM " + tableName + ".delete_files")
              .collectAsList();
      assertFalse(deleteFiles.isEmpty(), "DELETE should produce position delete files");
      long deletedRecords = 0;
      for (Row deleteFile : deleteFiles) {
        assertTrue(
            deleteFile.getString(0).endsWith(".orc"),
            "Delete file should be ORC: " + deleteFile.getString(0));
        deletedRecords += deleteFile.getLong(1);
      }
      assertEquals(2, deletedRecords, "Position delete files should cover the two deleted rows");

      spark.sql("DROP TABLE " + tableName);
    }
  }

  private static Map<String, String> tableProperties(SparkSession spark, String tableName) {
    return spark.sql("SHOW TBLPROPERTIES " + tableName).collectAsList().stream()
        .collect(Collectors.toMap(row -> row.getString(0), (Row row) -> row.getString(1)));
  }
}
