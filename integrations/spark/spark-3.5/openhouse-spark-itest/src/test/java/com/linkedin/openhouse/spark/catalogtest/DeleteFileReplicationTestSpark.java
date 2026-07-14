package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the delete file replication factor configuration introduced in
 * com.linkedin.iceberg 1.5.2.17 (linkedin/iceberg#219, #229, #254).
 *
 * <p>These tests drive the full OpenHouse stack through SQL: the table property / session conf must
 * round-trip through the OpenHouse tables service, merge-on-read DELETE on an ORC table must
 * produce ORC position delete files, and the delete files must actually be created with the
 * requested replication factor.
 *
 * <p>A local disk has no concept of block replication, so the SparkSession is configured with
 * {@link CaptureReplicationFileSystem} as {@code fs.file.impl}: a bare {@link RawLocalFileSystem}
 * that records the replication factor passed to {@code create(...)} — exactly what HDFS would
 * receive in production.
 *
 * <p>This only works because these two tests are executed in their own dedicated Gradle test task
 * ({@code deleteFileReplicationTest}), which runs in an isolated JVM/SparkContext: the {@code
 * fs.file.impl} Hadoop configuration is fixed at SparkContext-creation time, so this override must
 * never leak into the shared SparkSession used by the rest of this module's tests.
 */
public class DeleteFileReplicationTestSpark extends OpenHouseSparkITest {

  private static final String DATABASE = "d1_delete_repl";

  private SparkSession getReplicationCapturingSparkSession() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.hadoop.fs.file.impl", CaptureReplicationFileSystem.class.getName());
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
    try (SparkSession spark = getReplicationCapturingSparkSession()) {
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
    try (SparkSession spark = getReplicationCapturingSparkSession()) {
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
   * Verifies that every ORC position delete file produced for {@code tableName} was created with
   * {@code expectedReplication}, and that the delete files collectively cover the two deleted rows.
   */
  private void assertDeleteFilesHaveReplication(
      SparkSession spark, String tableName, short expectedReplication) {
    List<Row> deleteFiles =
        spark
            .sql("SELECT file_path, record_count FROM " + tableName + ".delete_files")
            .collectAsList();
    assertFalse(deleteFiles.isEmpty(), "DELETE should produce position delete files");

    long deletedRecords = 0;
    for (Row deleteFile : deleteFiles) {
      String filePath = deleteFile.getString(0);
      assertTrue(filePath.endsWith(".orc"), "Delete file should be ORC: " + filePath);
      deletedRecords += deleteFile.getLong(1);

      assertEquals(
          expectedReplication,
          CaptureReplicationFileSystem.capturedReplication(filePath),
          "Delete file " + filePath + " should be created with replication " + expectedReplication);
    }
    assertEquals(2, deletedRecords, "Position delete files should cover the two deleted rows");
  }

  private static Map<String, String> tableProperties(SparkSession spark, String tableName) {
    return spark.sql("SHOW TBLPROPERTIES " + tableName).collectAsList().stream()
        .collect(Collectors.toMap(row -> row.getString(0), (Row row) -> row.getString(1)));
  }

  /**
   * Local filesystem that records the replication factor passed to {@code create(...)}. The two
   * public create overloads below are independent entry points on {@link RawLocalFileSystem}, so
   * both are overridden; using {@link RawLocalFileSystem} directly keeps {@code ChecksumFileSystem}
   * out of the path (it would drop the replication argument before it reaches this class).
   */
  public static class CaptureReplicationFileSystem extends RawLocalFileSystem {
    // static because fs.file.impl.disable.cache makes FileSystem.get(...) return a new instance
    // per call: the instance used by Iceberg's write path and the one visible to assertions differ
    private static final Map<String, Short> CAPTURED = new ConcurrentHashMap<>();

    static short capturedReplication(String location) {
      Short replication = CAPTURED.get(new Path(location).getName());
      assertNotNull(replication, "No file creation was captured for " + location);
      return replication;
    }

    @Override
    public String getScheme() {
      // RawLocalFileSystem does not implement getScheme(), but Iceberg's locality check calls it
      return "file";
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(path, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(
          path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
  }
}
