package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.javaclient.OpenHouseCatalog;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * E2E test verifying that corrupt Iceberg metadata surfaces the real error to the client instead of
 * being masked as "Table does not exist", and that Iceberg does not delete committed files.
 *
 * <p>This test boots a real tables-service, creates a table with data, corrupts the metadata file
 * on disk, and verifies:
 *
 * <ol>
 *   <li>Reading from a corrupt table throws an error (not "Table does not exist")
 *   <li>Writing to a corrupt table throws an error
 *   <li>Data is fully intact after restoring the metadata — proving no Iceberg cleanup occurred
 * </ol>
 */
public class InvalidMetadataTest extends OpenHouseSparkITest {

  private static final String DATABASE = "invalid_metadata_test_db";

  @Test
  void testCorruptSchemaIdSurfacesRealError() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
      FileSystem fs = FileSystem.get(hadoopConf);
      String fqtn = "openhouse." + DATABASE + ".corrupt_test";

      // Step 1: Create a valid table and insert data
      spark.sql("CREATE TABLE " + fqtn + " (name string, id int)");
      spark.sql("INSERT INTO " + fqtn + " VALUES ('Alice', 1), ('Bob', 2)");
      long rowCountBefore = spark.sql("SELECT * FROM " + fqtn).count();
      Assertions.assertEquals(2, rowCountBefore, "Should have 2 rows before corruption");

      // Step 2: Get metadata file location and save original content
      OpenHouseCatalog catalog = (OpenHouseCatalog) getOpenHouseCatalog(spark);
      TableIdentifier tableId = TableIdentifier.of(DATABASE, "corrupt_test");
      TableOperations ops = catalog.newTableOps(tableId);
      TableMetadata metadata = ops.current();
      String metadataLocation = metadata.metadataFileLocation();
      Assertions.assertNotNull(metadataLocation, "Metadata file location should not be null");

      Path metadataPath = new Path(metadataLocation);
      String originalJson = readFile(fs, metadataPath);

      // Step 3: Corrupt the metadata file — change current-schema-id to a non-existent value
      String corruptJson =
          originalJson.replaceAll(
              "\"current-schema-id\"\\s*:\\s*\\d+", "\"current-schema-id\" : 999");
      Assertions.assertNotEquals(originalJson, corruptJson, "Metadata should have been modified");
      writeFile(fs, metadataPath, corruptJson);

      // Step 4: Read path — SELECT should fail with the real error, not "Table does not exist"
      // Refresh Spark's cached table metadata so it re-reads from the server
      spark.sql("REFRESH TABLE " + fqtn);
      Exception readException =
          Assertions.assertThrows(
              Exception.class, () -> spark.sql("SELECT * FROM " + fqtn).count());
      Assertions.assertTrue(
          readException.getMessage().contains("has invalid metadata"),
          "Read path should surface invalid metadata error, got: " + readException.getMessage());

      // Step 5: Write path — INSERT should also fail (refresh happens before commit)
      Exception writeException =
          Assertions.assertThrows(
              Exception.class, () -> spark.sql("INSERT INTO " + fqtn + " VALUES ('Charlie', 3)"));
      Assertions.assertTrue(
          writeException.getMessage().contains("has invalid metadata"),
          "Write path should surface invalid metadata error, got: " + writeException.getMessage());

      // Step 6: Restore the original metadata and verify all data is intact
      writeFile(fs, metadataPath, originalJson);
      long rowCountAfter = spark.sql("SELECT * FROM " + fqtn).count();
      Assertions.assertEquals(
          rowCountBefore,
          rowCountAfter,
          "All data should be intact after restoring metadata — no Iceberg cleanup occurred");
    }
  }

  private static String readFile(FileSystem fs, Path path) throws Exception {
    try (FSDataInputStream in = fs.open(path)) {
      byte[] bytes = new byte[(int) fs.getFileStatus(path).getLen()];
      in.readFully(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static void writeFile(FileSystem fs, Path path, String content) throws Exception {
    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }
}
