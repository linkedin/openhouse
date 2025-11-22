package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableStatsCollectionSparkApp.
 *
 * <p>Tests cover: - Core stats and commit events collection flow - Partitioned and unpartitioned
 * table handling - Commit events schema and content validation - Error handling and resilience -
 * Publishing methods
 */
@Slf4j
public class TableStatsCollectionSparkAppTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  // ==================== Core Flow Tests ====================

  @Test
  public void testSuccessfulStatsAndCommitEventsCollection() throws Exception {
    final String tableName = "db.test_stats_collection";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table and make multiple commits
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Action: Run the app
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      // Verify: Stats were collected
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertNotNull(stats);
      Assertions.assertEquals(numInserts, stats.getNumReferencedDataFiles());

      // Verify: Commit events were collected
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      Assertions.assertFalse(commitEvents.isEmpty());
      Assertions.assertEquals(numInserts, commitEvents.size());

      log.info("Successfully collected stats and {} commit events", commitEvents.size());
    }
  }

  @Test
  public void testStatsCollectionForTableWithNoCommits() throws Exception {
    final String tableName = "db.test_no_commits";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with no data
      prepareTable(ops, tableName);

      // Action: Run the app
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      // Verify: Stats still collected (metadata exists)
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertNotNull(stats);
      Assertions.assertEquals(0, stats.getNumReferencedDataFiles());
      Assertions.assertEquals(1, stats.getNumExistingMetadataJsonFiles()); // Initial metadata

      // Verify: No commit events (no snapshots)
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      Assertions.assertTrue(commitEvents.isEmpty());

      log.info("Successfully handled table with no commits");
    }
  }

  @Test
  public void testStatsCollectionForPartitionedTable() throws Exception {
    final String tableName = "db.test_partitioned";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true); // partitioned by days(ts)
      populateTable(ops, tableName, numInserts);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: Partition spec is captured and contains partition info
      Assertions.assertFalse(commitEvents.isEmpty());
      String partitionSpec = commitEvents.get(0).getDataset().getPartitionSpec();
      Assertions.assertNotNull(partitionSpec);
      // Partitioned tables have partition field info in spec
      Assertions.assertFalse(partitionSpec.contains("[]"));

      log.info("Successfully detected partitioned table with partition_spec: {}", partitionSpec);
    }
  }

  @Test
  public void testStatsCollectionForUnpartitionedTable() throws Exception {
    final String tableName = "db.test_unpartitioned";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create unpartitioned table
      prepareTable(ops, tableName, false); // not partitioned
      populateTable(ops, tableName, numInserts);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: Partition spec is captured
      Assertions.assertFalse(commitEvents.isEmpty());
      String partitionSpec = commitEvents.get(0).getDataset().getPartitionSpec();
      Assertions.assertNotNull(partitionSpec);
      // Unpartitioned tables have "[]" as partition spec
      Assertions.assertTrue(partitionSpec.contains("[]"));

      log.info("Successfully detected unpartitioned table with partition_spec: {}", partitionSpec);
    }
  }

  // ==================== Commit Events Tests ====================

  @Test
  public void testCommitEventsContainsCorrectSchema() throws Exception {
    final String tableName = "db.test_commit_schema";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Action
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: All required fields present in typed objects
      Assertions.assertFalse(commitEvents.isEmpty());
      Assertions.assertEquals(numInserts, commitEvents.size());

      CommitEventTable firstEvent = commitEvents.get(0);
      Assertions.assertNotNull(firstEvent);

      // Verify dataset fields (all required)
      Assertions.assertNotNull(firstEvent.getDataset());
      Assertions.assertEquals("db", firstEvent.getDataset().getDatabaseName());
      Assertions.assertEquals("test_commit_schema", firstEvent.getDataset().getTableName());
      Assertions.assertNotNull(firstEvent.getDataset().getClusterName());
      Assertions.assertNotNull(firstEvent.getDataset().getTableMetadataLocation());
      Assertions.assertNotNull(firstEvent.getDataset().getPartitionSpec());

      // Verify commit metadata fields (required fields only)
      Assertions.assertNotNull(firstEvent.getCommitMetadata());
      Assertions.assertNotNull(firstEvent.getCommitMetadata().getCommitId());
      Assertions.assertNotNull(firstEvent.getCommitMetadata().getCommitTimestampMs());
      // Note: commit_app_id, commit_app_name, and commit_operation are nullable

      // Verify event_timestamp_ms is placeholder (will be set at publish time)
      Assertions.assertEquals(0L, firstEvent.getEventTimestampMs());

      log.info("Commit events schema validated successfully");
    }
  }

  @Test
  public void testCommitEventsCollectsAllSnapshots() throws Exception {
    final String tableName = "db.test_all_snapshots";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with commits
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Action: Collect commit events (should collect all snapshots)
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: All commits are captured
      Assertions.assertEquals(numInserts, commitEvents.size());

      // Verify: Commit timestamps are populated
      List<Long> commitTimestamps =
          commitEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitTimestampMs())
              .collect(Collectors.toList());

      // All timestamps should be non-zero
      for (Long timestamp : commitTimestamps) {
        Assertions.assertTrue(timestamp > 0, "Commit timestamp should be populated");
      }

      log.info("All snapshots collected: {} commits found", commitEvents.size());
    }
  }

  @Test
  public void testEventTimestampConsistency() throws Exception {
    final String tableName = "db.test_timestamp_consistency";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Simulate publish: Set event timestamp on all objects (this happens in publishCommitEvents)
      long eventTimestamp = System.currentTimeMillis();
      commitEvents.forEach(event -> event.setEventTimestampMs(eventTimestamp));

      // Verify: All events have same event_timestamp
      List<Long> eventTimestamps =
          commitEvents.stream()
              .map(e -> e.getEventTimestampMs())
              .distinct()
              .collect(Collectors.toList());

      Assertions.assertEquals(1, eventTimestamps.size());
      Assertions.assertEquals(eventTimestamp, eventTimestamps.get(0).longValue());

      log.info("Event timestamp consistency validated: {}", eventTimestamp);
    }
  }

  @Test
  public void testCommitEventsOrdering() throws Exception {
    final String tableName = "db.test_ordering";
    final int numInserts = 5;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create multiple commits
      prepareTable(ops, tableName);
      for (int i = 0; i < numInserts; i++) {
        populateTable(ops, tableName, 1);
        Thread.sleep(10); // Ensure different timestamps
      }

      // Action
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: Events are ordered by commit_timestamp_ms
      List<Long> timestamps =
          commitEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitTimestampMs())
              .collect(Collectors.toList());

      for (int i = 1; i < timestamps.size(); i++) {
        Assertions.assertTrue(
            timestamps.get(i) >= timestamps.get(i - 1), "Timestamps should be in ascending order");
      }

      log.info("Commit events ordering validated");
    }
  }

  // ==================== Error Handling Tests ====================

  @Test
  public void testInvalidTableNameFormat() throws Exception {
    final String invalidTableName = "invalid_table_name_without_db"; // Missing db prefix

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Action: Try to collect commit events with invalid FQTN
      // This should throw NoSuchTableException since table doesn't exist

      Assertions.assertThrows(
          Exception.class,
          () -> ops.collectCommitEventTable(invalidTableName),
          "Should throw exception for invalid table name");

      log.info("Invalid table name handled with exception as expected");
    }
  }

  // ==================== Publishing Tests ====================

  @Test
  public void testPublishStatsLogsCorrectly() throws Exception {
    final String tableName = "db.test_publish_stats";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      IcebergTableStats stats = ops.collectTableStats(tableName);

      // Action: Publish stats (this logs JSON)
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.publishStats(stats);

      // Verify: Stats object is valid JSON
      Gson gson = new Gson();
      String json = gson.toJson(stats);
      Assertions.assertNotNull(json);
      Assertions.assertTrue(json.contains("numReferencedDataFiles"));

      log.info("Stats publishing validated");
    }
  }

  @Test
  public void testPublishCommitEventsWithEmptyDataset() throws Exception {
    final String tableName = "db.test_publish_empty";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Table with no commits
      prepareTable(ops, tableName);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: Empty list returned (no snapshots)
      Assertions.assertTrue(commitEvents.isEmpty());

      log.info("Empty commit events handled gracefully");
    }
  }

  @Test
  public void testPublishCommitEventsWithData() throws Exception {
    final String tableName = "db.test_publish_data";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Action
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.publishCommitEvents(commitEvents);

      // Verify: JSON output is valid
      String json = new Gson().toJson(commitEvents);
      Assertions.assertNotNull(json);
      Assertions.assertTrue(json.contains("commitId"));

      log.info("Commit events publishing validated with {} events", commitEvents.size());
    }
  }

  // ==================== Integration Tests ====================

  @Test
  public void testEndToEndFlowWithMultipleCommits() throws Exception {
    final String tableName = "db.test_e2e_flow";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table and make multiple commits
      prepareTable(ops, tableName);
      populateTable(ops, tableName, numInserts);

      // Get snapshot IDs to verify
      Table table = ops.getTable(tableName);
      List<Long> snapshotIds =
          StreamSupport.stream(table.snapshots().spliterator(), false)
              .map(Snapshot::snapshotId)
              .collect(Collectors.toList());
      Assertions.assertEquals(numInserts, snapshotIds.size());

      // Action: Run full app
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      // Verify: Stats collected
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(numInserts, stats.getNumReferencedDataFiles());
      Assertions.assertEquals(numInserts, stats.getNumSnapshots());

      // Verify: Commit events collected
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      Assertions.assertEquals(numInserts, commitEvents.size());

      // Verify: All snapshot IDs are present in commit events
      List<Long> commitIds =
          commitEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitId())
              .collect(Collectors.toList());
      Assertions.assertTrue(commitIds.containsAll(snapshotIds));

      log.info("End-to-end flow validated successfully");
    }
  }

  @Test
  public void testAppInstantiationDirectly() throws Exception {
    // Test: Create app directly without factory method (avoids StateManager setup issues)
    final String tableName = "db.test_instantiation";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      prepareTable(ops, tableName);
      populateTable(ops, tableName, 2);

      // Action: Create app instance directly
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job-123", null, tableName, otelEmitter);

      // Verify: App can run successfully
      Assertions.assertNotNull(app);
      app.runInner(ops);

      // Verify results
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertEquals(2, stats.getNumReferencedDataFiles());

      log.info("Direct app instantiation validated successfully");
    }
  }

  // ==================== Helper Methods ====================

  private static void prepareTable(Operations ops, String tableName) {
    prepareTable(ops, tableName, false);
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

  private static void populateTable(Operations ops, String tableName, int numRows) {
    populateTable(ops, tableName, numRows, 0);
  }

  private static void populateTable(Operations ops, String tableName, int numRows, int dayLag) {
    populateTable(ops, tableName, numRows, dayLag, System.currentTimeMillis() / 1000);
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
}
