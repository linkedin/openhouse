package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the delete file replication factor configuration introduced in
 * com.linkedin.iceberg 1.5.2.17 (linkedin/iceberg#219, #229, #254).
 *
 * <p>These tests drive the full OpenHouse stack through SQL: the table property / session conf must
 * round-trip through the OpenHouse tables service, merge-on-read DELETE on an ORC table must
 * produce ORC position delete files, and — critically — the delete files must actually be created
 * with the requested replication factor.
 *
 * <p>Hadoop's stock local filesystem support silently discards any requested replication factor (a
 * local disk has no concept of block replication), so the SparkSession created by these tests is
 * configured with {@link ReplicationTrackingLocalFileSystem}, a {@code fs.file.impl} override that
 * disables checksums and records the replication factor requested at file-creation time, surfacing
 * it back through the standard HadoopFS {@code getFileStatus(Path)} API. This lets these tests
 * assert on the real replication factor Iceberg's write path requested, not merely that a property
 * round-tripped through table metadata.
 *
 * <p>This only works because these two tests are executed in their own dedicated Gradle test task
 * ({@code deleteFileReplicationTest}), which runs in an isolated JVM/SparkContext: the {@code
 * fs.file.impl} Hadoop configuration is fixed at SparkContext-creation time, so this override must
 * never leak into the shared SparkSession used by the rest of this module's tests.
 */
public class DeleteFileReplicationTestSpark extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_delete_repl";

  private SparkSession getReplicationTrackingSparkSession() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.hadoop.fs.file.impl", ReplicationTrackingLocalFileSystem.class.getName());
    // Hadoop's FileSystem.get(...) caches instances process-wide, keyed only by URI/UGI, not by
    // the Configuration used to create them. LocalStorageClient's own local filesystem is
    // initialized (and cached) before this SparkSession exists, so without disabling the cache
    // here, our fs.file.impl override above would silently be ignored and Spark would reuse the
    // already-cached stock LocalFileSystem instance instead.
    overrides.put("spark.hadoop.fs.file.impl.disable.cache", "true");
    return getSparkSession("openhouse", overrides);
  }

  @Test
  public void testDeleteFileReplicationFromTableProperty() throws Exception {
    String tableName = "openhouse." + DATABASE + ".orc_tbl_prop";
    try (SparkSession spark = getReplicationTrackingSparkSession()) {
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

      // the delete files must actually be created with the configured replication factor
      assertDeleteFilesHaveReplication(spark, tableName, (short) 5);

      spark.sql("DROP TABLE " + tableName);
    }
  }

  @Test
  public void testDeleteFileReplicationFromSessionConf() throws Exception {
    String tableName = "openhouse." + DATABASE + ".orc_tbl_conf";
    String confKey = "spark.sql.iceberg.delete-file-replication";
    try (SparkSession spark = getReplicationTrackingSparkSession()) {
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

      // the delete files must actually be created with the configured replication factor
      assertDeleteFilesHaveReplication(spark, tableName, (short) 7);

      spark.sql("DROP TABLE " + tableName);
    }
  }

  /**
   * Verifies, via HadoopFS, that every ORC position delete file produced for {@code tableName} was
   * actually created with {@code expectedReplication}, and that the delete files collectively cover
   * the two deleted rows.
   */
  private void assertDeleteFilesHaveReplication(
      SparkSession spark, String tableName, short expectedReplication) throws Exception {
    List<Row> deleteFiles =
        spark
            .sql("SELECT file_path, record_count FROM " + tableName + ".delete_files")
            .collectAsList();
    assertFalse(deleteFiles.isEmpty(), "DELETE should produce position delete files");

    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConf);
    assertTrue(
        fs instanceof ReplicationTrackingLocalFileSystem,
        "Test filesystem override did not apply; expected a fresh SparkContext with "
            + "fs.file.impl set to ReplicationTrackingLocalFileSystem, got: "
            + fs.getClass().getName());
    ReplicationTrackingLocalFileSystem trackingFs = (ReplicationTrackingLocalFileSystem) fs;

    long deletedRecords = 0;
    for (Row deleteFile : deleteFiles) {
      String filePath = deleteFile.getString(0);
      assertTrue(filePath.endsWith(".orc"), "Delete file should be ORC: " + filePath);
      deletedRecords += deleteFile.getLong(1);

      short actualReplication = trackingFs.getRequestedReplication(new Path(filePath));
      assertEquals(
          expectedReplication,
          actualReplication,
          "Delete file "
              + filePath
              + " should have been created with replication factor "
              + expectedReplication
              + " but was created with "
              + actualReplication);
    }
    assertEquals(2, deletedRecords, "Position delete files should cover the two deleted rows");
  }

  private static Map<String, String> tableProperties(SparkSession spark, String tableName) {
    return spark.sql("SHOW TBLPROPERTIES " + tableName).collectAsList().stream()
        .collect(Collectors.toMap(row -> row.getString(0), (Row row) -> row.getString(1)));
  }
}
