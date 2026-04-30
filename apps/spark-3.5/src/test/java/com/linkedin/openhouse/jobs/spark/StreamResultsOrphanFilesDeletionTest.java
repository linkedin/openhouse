package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test for stream-results option in orphan files deletion. This test runs only on Spark
 * 3.5 with Iceberg 1.5 where the streaming implementation is available. It verifies that
 * stream-results mode caps the returned paths at maxOrphanFileSampleSize while still deleting all
 * orphan files, and that non-streaming mode returns all paths regardless of sample size.
 */
@Slf4j
public class StreamResultsOrphanFilesDeletionTest extends OpenHouseSparkITest {
  private static final String BACKUP_DIR = ".backup";
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  private void prepareTable(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark()
        .sql(String.format("CREATE TABLE openhouse.%s (data string, ts timestamp)", tableName))
        .show();
  }

  private void populateTable(Operations ops, String tableName, int numRows) {
    for (int row = 0; row < numRows; ++row) {
      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('v%d', current_timestamp())", tableName, row))
          .show();
    }
  }

  @Test
  public void testStreamResultsCapsReturnedPaths() throws Exception {
    final String tableName = "db.test_stream_results_ofd";
    final int numOrphanFiles = 10;
    final int maxSampleSize = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, 3);
      Table table = ops.getTable(tableName);
      FileSystem fs = ops.fs();
      for (int i = 0; i < numOrphanFiles; i++) {
        fs.createNewFile(new Path(table.location(), "data/orphan_stream_" + i + ".orc"));
      }
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(
              table, System.currentTimeMillis(), false, BACKUP_DIR, 1, true, maxSampleSize);
      List<String> paths = Lists.newArrayList(result.orphanFileLocations().iterator());
      // Streaming mode caps returned paths at maxSampleSize
      Assertions.assertEquals(
          maxSampleSize, paths.size(), "Streaming should return at most maxSampleSize paths");
      // All orphan files should still be deleted
      for (int i = 0; i < numOrphanFiles; i++) {
        Assertions.assertFalse(
            fs.exists(new Path(table.location(), "data/orphan_stream_" + i + ".orc")),
            "Orphan file should be deleted");
      }
    }
  }

  @Test
  public void testNonStreamReturnsAllPaths() throws Exception {
    final String tableName = "db.test_non_stream_ofd";
    final int numOrphanFiles = 10;
    final int maxSampleSize = 3;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, 3);
      Table table = ops.getTable(tableName);
      FileSystem fs = ops.fs();
      for (int i = 0; i < numOrphanFiles; i++) {
        fs.createNewFile(new Path(table.location(), "data/orphan_nostream_" + i + ".orc"));
      }
      DeleteOrphanFiles.Result result =
          ops.deleteOrphanFiles(
              table, System.currentTimeMillis(), false, BACKUP_DIR, 1, false, maxSampleSize);
      List<String> paths = Lists.newArrayList(result.orphanFileLocations().iterator());
      // Non-streaming ignores maxSampleSize - returns all paths
      Assertions.assertEquals(
          numOrphanFiles, paths.size(), "Non-streaming should return all orphan file paths");
      // All orphan files should be deleted
      for (int i = 0; i < numOrphanFiles; i++) {
        Assertions.assertFalse(
            fs.exists(new Path(table.location(), "data/orphan_nostream_" + i + ".orc")),
            "Orphan file should be deleted");
      }
    }
  }
}
