package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.ColumnData;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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

      // Verify event_timestamp_ms is set at collection time
      Assertions.assertTrue(firstEvent.getEventTimestampMs() > 0);

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

  // ==================== Commit Metadata (Spark/Trino) Tests ====================
  //
  // These tests validate the coalesce logic for commitAppId and commitAppName:
  // - commitAppId = coalesce(spark.app.id, trino_query_id)
  // - commitAppName = when(spark.app.id.isNotNull, spark.app.name)
  //                   .when(trino_query_id.isNotNull, "trino")
  //
  // We use the Iceberg Table API directly to create commits with controlled
  // summary properties, bypassing Spark SQL which automatically sets spark.app.id.

  /**
   * Creates a DataFile for testing commits with controlled summary properties. The file doesn't
   * need to physically exist since we're testing metadata collection.
   */
  private DataFile createTestDataFile(Table table) {
    PartitionSpec spec = table.spec();
    Schema schema = table.schema();

    if (spec.isUnpartitioned()) {
      return DataFiles.builder(spec)
          .withPath("/test/data-" + System.nanoTime() + ".parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(1)
          .build();
    } else {
      // For partitioned tables, we need to provide partition data
      // Our test tables are partitioned by days(ts), so partition path is like "ts_day=19000"
      return DataFiles.builder(spec)
          .withPath("/test/data-" + System.nanoTime() + ".parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(1)
          .withPartitionPath("ts_day=19000")
          .build();
    }
  }

  @Test
  public void testCommitMetadata_BothSparkAndTrinoNull() throws Exception {
    final String tableName = "db.test_commit_metadata_both_null";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table
      prepareTable(ops, tableName);

      // Create a commit using Iceberg Table API directly (without Spark SQL)
      // This bypasses Spark's automatic spark.app.id/spark.app.name injection
      Table table = ops.getTable(tableName);
      DataFile dataFile = createTestDataFile(table);
      table.newAppend().appendFile(dataFile).commit();

      // Verify the commit actually succeeded by checking snapshot exists
      table.refresh();
      Assertions.assertNotNull(table.currentSnapshot(), "Commit should have created a snapshot");
      log.info("Commit succeeded with snapshot ID: {}", table.currentSnapshot().snapshotId());

      // Refresh Spark's catalog cache to see the new metadata
      ops.spark().sql("REFRESH TABLE " + tableName);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: Both commitAppId and commitAppName should be null
      // when neither spark.app.id nor trino_query_id is present in the summary
      Assertions.assertFalse(commitEvents.isEmpty(), "Should have at least one commit event");
      CommitEventTable event = commitEvents.get(0);

      Assertions.assertNull(
          event.getCommitMetadata().getCommitAppId(),
          "commitAppId should be null when both spark.app.id and trino_query_id are absent");
      Assertions.assertNull(
          event.getCommitMetadata().getCommitAppName(),
          "commitAppName should be null when both spark.app.id and trino_query_id are absent");

      log.info(
          "Both null scenario validated: commitAppId={}, commitAppName={}",
          event.getCommitMetadata().getCommitAppId(),
          event.getCommitMetadata().getCommitAppName());
    }
  }

  @Test
  public void testCommitMetadata_SparkNullTrinoPresent() throws Exception {
    final String tableName = "db.test_commit_metadata_trino_only";
    final String trinoQueryId = "20240101_123456_00001_abcde";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table
      prepareTable(ops, tableName);

      // Create a commit using Iceberg Table API with only trino_query_id set
      // This simulates a Trino commit where spark.app.id is not present
      Table table = ops.getTable(tableName);
      DataFile dataFile = createTestDataFile(table);
      table.newAppend().appendFile(dataFile).set("trino_query_id", trinoQueryId).commit();

      // Verify the commit actually succeeded by checking snapshot exists
      table.refresh();
      Assertions.assertNotNull(table.currentSnapshot(), "Commit should have created a snapshot");
      log.info("Commit succeeded with snapshot ID: {}", table.currentSnapshot().snapshotId());

      // Refresh Spark's catalog cache to see the new metadata
      ops.spark().sql("REFRESH TABLE " + tableName);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: commitAppId should be trino_query_id, commitAppName should be "trino"
      Assertions.assertFalse(commitEvents.isEmpty(), "Should have at least one commit event");
      CommitEventTable event = commitEvents.get(0);

      Assertions.assertEquals(
          trinoQueryId,
          event.getCommitMetadata().getCommitAppId(),
          "commitAppId should be trino_query_id when spark.app.id is null");
      Assertions.assertEquals(
          "trino",
          event.getCommitMetadata().getCommitAppName(),
          "commitAppName should be 'trino' when trino_query_id is present and spark.app.id is null");

      log.info(
          "Trino-only scenario validated: commitAppId={}, commitAppName={}",
          event.getCommitMetadata().getCommitAppId(),
          event.getCommitMetadata().getCommitAppName());
    }
  }

  @Test
  public void testCommitMetadata_SparkPresentTrinoNull() throws Exception {
    final String tableName = "db.test_commit_metadata_spark_only";
    final String sparkAppId = "local-1704067200000";
    final String sparkAppName = "TestSparkApp";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table
      prepareTable(ops, tableName);

      // Create a commit using Iceberg Table API with only spark.app.id and spark.app.name set
      // This simulates a Spark commit where trino_query_id is not present
      Table table = ops.getTable(tableName);
      DataFile dataFile = createTestDataFile(table);
      table
          .newAppend()
          .appendFile(dataFile)
          .set("spark.app.id", sparkAppId)
          .set("spark.app.name", sparkAppName)
          .commit();

      // Verify the commit actually succeeded by checking snapshot exists
      table.refresh();
      Assertions.assertNotNull(table.currentSnapshot(), "Commit should have created a snapshot");
      log.info("Commit succeeded with snapshot ID: {}", table.currentSnapshot().snapshotId());

      // Refresh Spark's catalog cache to see the new metadata
      ops.spark().sql("REFRESH TABLE " + tableName);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: commitAppId should be spark.app.id, commitAppName should be spark.app.name
      Assertions.assertFalse(commitEvents.isEmpty(), "Should have at least one commit event");
      CommitEventTable event = commitEvents.get(0);

      Assertions.assertEquals(
          sparkAppId,
          event.getCommitMetadata().getCommitAppId(),
          "commitAppId should be spark.app.id when present");
      Assertions.assertEquals(
          sparkAppName,
          event.getCommitMetadata().getCommitAppName(),
          "commitAppName should be spark.app.name when spark.app.id is present");

      log.info(
          "Spark-only scenario validated: commitAppId={}, commitAppName={}",
          event.getCommitMetadata().getCommitAppId(),
          event.getCommitMetadata().getCommitAppName());
    }
  }

  @Test
  public void testCommitMetadata_BothPresentSparkTakesPrecedence() throws Exception {
    final String tableName = "db.test_commit_metadata_both_present";
    final String sparkAppId = "local-1704067200000";
    final String sparkAppName = "TestSparkApp";
    final String trinoQueryId = "20240101_123456_00001_abcde";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table
      prepareTable(ops, tableName);

      // Create a commit using Iceberg Table API with BOTH spark.app.id and trino_query_id set
      // This edge case shouldn't happen in production but we verify Spark takes precedence
      Table table = ops.getTable(tableName);
      DataFile dataFile = createTestDataFile(table);
      table
          .newAppend()
          .appendFile(dataFile)
          .set("spark.app.id", sparkAppId)
          .set("spark.app.name", sparkAppName)
          .set("trino_query_id", trinoQueryId)
          .commit();

      // Verify the commit actually succeeded by checking snapshot exists
      table.refresh();
      Assertions.assertNotNull(table.currentSnapshot(), "Commit should have created a snapshot");
      log.info("Commit succeeded with snapshot ID: {}", table.currentSnapshot().snapshotId());

      // Refresh Spark's catalog cache to see the new metadata
      ops.spark().sql("REFRESH TABLE " + tableName);

      // Action: Collect commit events
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);

      // Verify: spark.app.id takes precedence due to coalesce() ordering
      // commitAppId = coalesce(spark.app.id, trino_query_id) → spark.app.id
      // commitAppName = when(spark.app.id.isNotNull, spark.app.name) → spark.app.name
      Assertions.assertFalse(commitEvents.isEmpty(), "Should have at least one commit event");
      CommitEventTable event = commitEvents.get(0);

      Assertions.assertEquals(
          sparkAppId,
          event.getCommitMetadata().getCommitAppId(),
          "commitAppId should be spark.app.id (takes precedence over trino_query_id)");
      Assertions.assertEquals(
          sparkAppName,
          event.getCommitMetadata().getCommitAppName(),
          "commitAppName should be spark.app.name when spark.app.id is present");

      // Verify it's NOT using the Trino values
      Assertions.assertNotEquals(
          trinoQueryId,
          event.getCommitMetadata().getCommitAppId(),
          "commitAppId should NOT be trino_query_id when spark.app.id is also present");
      Assertions.assertNotEquals(
          "trino",
          event.getCommitMetadata().getCommitAppName(),
          "commitAppName should NOT be 'trino' when spark.app.id is also present");

      log.info(
          "Both present scenario validated - Spark takes precedence: commitAppId={}, commitAppName={}",
          event.getCommitMetadata().getCommitAppId(),
          event.getCommitMetadata().getCommitAppName());
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

  // ==================== Partition Events Tests ====================

  @Test
  public void testPartitionEventsForPartitionedTable() throws Exception {
    final String tableName = "db.test_partition_events";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table with data in different partitions
      prepareTable(ops, tableName, true); // partitioned by days(ts)

      // Insert data into 3 different partitions (3 commits, 3 partitions)
      populateTable(ops, tableName, 1, 0); // Today
      populateTable(ops, tableName, 1, 1); // Yesterday
      populateTable(ops, tableName, 1, 2); // 2 days ago

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      // Verify: Should have 3 partition events (1 per commit-partition pair)
      Assertions.assertFalse(partitionEvents.isEmpty());
      Assertions.assertEquals(numInserts, partitionEvents.size());

      // Verify: All events have partition data
      for (CommitEventTablePartitions event : partitionEvents) {
        Assertions.assertNotNull(event.getPartitionData());
        Assertions.assertFalse(event.getPartitionData().isEmpty());

        // Verify: Partition data has at least one column
        Assertions.assertTrue(event.getPartitionData().size() > 0);

        // Verify: Partition values are typed ColumnData
        ColumnData firstColumn = event.getPartitionData().get(0);
        Assertions.assertNotNull(firstColumn);
        Assertions.assertNotNull(firstColumn.getColumnName());
      }

      log.info("Partition events collection validated: {} events", partitionEvents.size());
    }
  }

  @Test
  public void testPartitionEventsForUnpartitionedTable() throws Exception {
    final String tableName = "db.test_unpartitioned_partition_events";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create unpartitioned table
      prepareTable(ops, tableName, false); // NOT partitioned
      populateTable(ops, tableName, 2);

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      // Verify: Empty list for unpartitioned tables
      Assertions.assertTrue(
          partitionEvents.isEmpty(), "Unpartitioned table should return empty partition events");

      log.info("Unpartitioned table correctly returns empty partition events");
    }
  }

  @Test
  public void testPartitionEventsSchemaValidation() throws Exception {
    final String tableName = "db.test_partition_schema";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, 1);

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      Assertions.assertFalse(partitionEvents.isEmpty());
      CommitEventTablePartitions firstEvent = partitionEvents.get(0);

      // Verify: Dataset fields
      Assertions.assertNotNull(firstEvent.getDataset());
      Assertions.assertEquals("db", firstEvent.getDataset().getDatabaseName());
      Assertions.assertEquals("test_partition_schema", firstEvent.getDataset().getTableName());
      Assertions.assertNotNull(firstEvent.getDataset().getClusterName());
      Assertions.assertNotNull(firstEvent.getDataset().getTableMetadataLocation());
      Assertions.assertNotNull(firstEvent.getDataset().getPartitionSpec());

      // Verify: Commit metadata fields
      Assertions.assertNotNull(firstEvent.getCommitMetadata());
      Assertions.assertNotNull(firstEvent.getCommitMetadata().getCommitId());
      Assertions.assertNotNull(firstEvent.getCommitMetadata().getCommitTimestampMs());
      Assertions.assertTrue(firstEvent.getCommitMetadata().getCommitTimestampMs() > 0);

      // Verify: Partition data (required for partitioned tables)
      Assertions.assertNotNull(firstEvent.getPartitionData());
      Assertions.assertFalse(firstEvent.getPartitionData().isEmpty());

      // Verify: Event timestamp is set at collection time
      Assertions.assertTrue(firstEvent.getEventTimestampMs() > 0);

      log.info("Partition events schema validated successfully");
    }
  }

  @Test
  public void testPartitionEventsWithMultiplePartitions() throws Exception {
    final String tableName = "db.test_multiple_partitions";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table and insert into multiple partitions
      prepareTable(ops, tableName, true);

      // Insert 2 rows (creates 2 separate commits, same partition)
      populateTable(ops, tableName, 2, 0); // 2 commits to partition day=0
      // Insert 2 rows (creates 2 separate commits, different partition)
      populateTable(ops, tableName, 2, 1); // 2 commits to partition day=1
      // Insert 1 row (creates 1 commit, yet another partition)
      populateTable(ops, tableName, 1, 2); // 1 commit to partition day=2

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      // Verify: Should have 5 partition events (1 per commit-partition pair)
      // Note: populateTable creates 1 commit per row, so 2+2+1 = 5 commits total
      Assertions.assertEquals(5, partitionEvents.size());

      // Verify: All commit IDs are present
      List<Long> commitIds =
          partitionEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitId())
              .collect(Collectors.toList());
      Assertions.assertEquals(5, commitIds.size());

      // Verify: Commit IDs are unique (each INSERT creates a unique commit)
      long uniqueCommitIds = commitIds.stream().distinct().count();
      Assertions.assertEquals(5, uniqueCommitIds);

      log.info(
          "Multiple partitions handled correctly: {} partition events", partitionEvents.size());
    }
  }

  @Test
  public void testPartitionDataTypeHandling() throws Exception {
    final String tableName = "db.test_partition_types";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, 1);

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      Assertions.assertFalse(partitionEvents.isEmpty());
      CommitEventTablePartitions event = partitionEvents.get(0);

      // Verify: Partition data contains typed ColumnData objects
      List<ColumnData> partitionData = event.getPartitionData();
      Assertions.assertNotNull(partitionData);
      Assertions.assertFalse(partitionData.isEmpty());

      // Verify: Each column has name and value
      for (ColumnData columnData : partitionData) {
        Assertions.assertNotNull(columnData.getColumnName());

        // Partition columns in our test are days(ts) which produces Integer/Long values
        // Verify it's one of the supported types
        boolean isValidType =
            columnData instanceof ColumnData.LongColumnData
                || columnData instanceof ColumnData.DoubleColumnData
                || columnData instanceof ColumnData.StringColumnData;

        Assertions.assertTrue(
            isValidType, "Partition column should be one of the supported ColumnData types");
      }

      log.info("Partition data types validated successfully");
    }
  }

  @Test
  public void testPublishPartitionEvents() throws Exception {
    final String tableName = "db.test_publish_partition_events";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, 2, 0);
      populateTable(ops, tableName, 1, 1);

      // Action: Collect and publish partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.publishPartitionEvents(partitionEvents);

      // Verify: Event timestamps are set after publishing
      long publishTime = System.currentTimeMillis();
      for (CommitEventTablePartitions event : partitionEvents) {
        Assertions.assertTrue(
            event.getEventTimestampMs() > 0, "Event timestamp should be set after publishing");
        Assertions.assertTrue(
            event.getEventTimestampMs() <= publishTime,
            "Event timestamp should not be in the future");
      }

      // Verify: JSON serialization works
      String json = new Gson().toJson(partitionEvents);
      Assertions.assertNotNull(json);
      Assertions.assertTrue(json.contains("partitionData"));
      Assertions.assertTrue(json.contains("commitId"));

      log.info("Partition events publishing validated with {} events", partitionEvents.size());
    }
  }

  @Test
  public void testPartitionEventsIntegrationWithFullApp() throws Exception {
    final String tableName = "db.test_partition_integration";
    final int numCommits = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table with multiple commits
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, 1, 0); // Commit 1, partition 1
      populateTable(ops, tableName, 1, 1); // Commit 2, partition 2

      // Action: Run full app (should collect stats, commit events, AND partition events)
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      // Verify: Stats collected
      IcebergTableStats stats = ops.collectTableStats(tableName);
      Assertions.assertNotNull(stats);
      Assertions.assertEquals(numCommits, stats.getNumReferencedDataFiles());

      // Verify: Commit events collected
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      Assertions.assertEquals(numCommits, commitEvents.size());

      // Verify: Partition events collected
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);
      Assertions.assertEquals(numCommits, partitionEvents.size());

      // Verify: Commit IDs match between commit events and partition events
      List<Long> commitEventIds =
          commitEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitId())
              .sorted()
              .collect(Collectors.toList());

      List<Long> partitionEventCommitIds =
          partitionEvents.stream()
              .map(e -> e.getCommitMetadata().getCommitId())
              .sorted()
              .collect(Collectors.toList());

      Assertions.assertEquals(
          commitEventIds,
          partitionEventCommitIds,
          "Commit IDs should match between commit events and partition events");

      log.info(
          "Full app integration validated: {} commits, {} partition events",
          commitEvents.size(),
          partitionEvents.size());
    }
  }

  @Test
  public void testPartitionEventsWithNoData() throws Exception {
    final String tableName = "db.test_partition_no_data";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table with no data
      prepareTable(ops, tableName, true);

      // Action: Collect partition events
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      // Verify: Empty list (no commits = no partition events)
      Assertions.assertTrue(
          partitionEvents.isEmpty(), "Table with no data should return empty partition events");

      log.info("Empty table handled correctly for partition events");
    }
  }

  @Test
  public void testPartitionEventsParallelExecution() throws Exception {
    final String tableName = "db.test_parallel_execution";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, 2, 0);

      // Action: Run app which executes all three collections in parallel
      long startTime = System.currentTimeMillis();

      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      long endTime = System.currentTimeMillis();
      long parallelExecutionTime = endTime - startTime;

      // Verify: All three collections completed
      IcebergTableStats stats = ops.collectTableStats(tableName);
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      Assertions.assertNotNull(stats);
      Assertions.assertFalse(commitEvents.isEmpty());
      Assertions.assertFalse(partitionEvents.isEmpty());

      // Note: We can't easily verify parallel execution is faster without baseline,
      // but we can verify all three completed successfully
      log.info(
          "Parallel execution completed in {} ms: stats={}, commits={}, partitions={}",
          parallelExecutionTime,
          stats != null,
          commitEvents.size(),
          partitionEvents.size());
    }
  }

  // ==================== Partition Stats Tests (NEW) ====================

  @Test
  public void testPartitionStatsForPartitionedTable() throws Exception {
    final String tableName = "db.test_partition_stats";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, numInserts);

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Partition stats collected
      Assertions.assertFalse(partitionStats.isEmpty(), "Partition stats should not be empty");

      // Verify: Each partition has latest commit only (not all commits)
      Assertions.assertTrue(
          partitionStats.size() <= numInserts,
          "Partition stats should have at most one record per unique partition");

      // Verify: Each stat has required fields
      for (com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat :
          partitionStats) {
        Assertions.assertNotNull(stat.getDataset(), "Dataset should not be null");
        Assertions.assertNotNull(stat.getCommitMetadata(), "Commit metadata should not be null");
        Assertions.assertNotNull(stat.getPartitionData(), "Partition data should not be null");
        Assertions.assertNotNull(stat.getRowCount(), "Row count should not be null");
        Assertions.assertNotNull(stat.getColumnCount(), "Column count should not be null");

        // Verify: Stats include column-level metrics
        Assertions.assertNotNull(stat.getNullCount(), "Null count list should not be null");
        Assertions.assertNotNull(stat.getNanCount(), "NaN count list should not be null");
        Assertions.assertNotNull(stat.getMinValue(), "Min value list should not be null");
        Assertions.assertNotNull(stat.getMaxValue(), "Max value list should not be null");
        Assertions.assertNotNull(
            stat.getColumnSizeInBytes(), "Column size list should not be null");

        log.info(
            "Partition stats: partition={}, rowCount={}, columnCount={}, commitId={}",
            stat.getPartitionData(),
            stat.getRowCount(),
            stat.getColumnCount(),
            stat.getCommitMetadata().getCommitId());
      }

      log.info("Collected {} partition stats for partitioned table", partitionStats.size());
    }
  }

  @Test
  public void testPartitionStatsForUnpartitionedTable() throws Exception {
    final String tableName = "db.test_unpartitioned_stats";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create unpartitioned table
      prepareTable(ops, tableName, false);
      populateTable(ops, tableName, numInserts);

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Single stats record for unpartitioned table
      Assertions.assertEquals(
          1, partitionStats.size(), "Unpartitioned table should return exactly one stats record");

      com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat =
          partitionStats.get(0);

      // Verify: Partition data is empty for unpartitioned table
      Assertions.assertTrue(
          stat.getPartitionData().isEmpty(),
          "Unpartitioned table should have empty partition data");

      // Verify: Row count includes all data
      Assertions.assertTrue(
          stat.getRowCount() > 0, "Row count should be positive for table with data");

      // Verify: Has current snapshot metadata
      Assertions.assertNotNull(
          stat.getCommitMetadata(), "Commit metadata should not be null for unpartitioned");
      Assertions.assertNotNull(
          stat.getCommitMetadata().getCommitId(), "Snapshot ID should not be null");

      log.info(
          "Collected stats for unpartitioned table: rowCount={}, commitId={}",
          stat.getRowCount(),
          stat.getCommitMetadata().getCommitId());
    }
  }

  @Test
  public void testPartitionStatsWithMultiplePartitions() throws Exception {
    final String tableName = "db.test_multiple_partitions";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);

      // Insert data into different partitions (different days)
      populateTable(ops, tableName, 1, 0); // Day 0
      populateTable(ops, tableName, 1, 1); // Day 1
      populateTable(ops, tableName, 1, 2); // Day 2

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Each partition has one stats record (latest commit)
      Assertions.assertEquals(
          3, partitionStats.size(), "Should have 3 partition stats (one per unique partition)");

      // Verify: All partitions have different partition data
      java.util.Set<String> uniquePartitions = new java.util.HashSet<>();
      for (com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat :
          partitionStats) {
        String partitionKey = stat.getPartitionData().toString();
        uniquePartitions.add(partitionKey);
      }

      Assertions.assertEquals(
          3, uniquePartitions.size(), "All 3 partitions should have unique partition data");

      log.info("Collected stats for {} unique partitions", partitionStats.size());
    }
  }

  @Test
  public void testPartitionStatsWithNestedColumns() throws Exception {
    final String tableName = "db.test_nested_columns";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with nested column
      ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (id bigint, user struct<name:string, age:int>, ts timestamp) "
                      + "partitioned by (days(ts))",
                  tableName))
          .show();

      // Insert data
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES (1, named_struct('name', 'Alice', 'age', 30), current_timestamp())",
                  tableName))
          .show();

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Stats collected for table with nested columns
      Assertions.assertFalse(partitionStats.isEmpty(), "Should collect stats for nested columns");

      com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat =
          partitionStats.get(0);

      // Verify: Column count includes flattened nested columns if available in readable_metrics
      Assertions.assertTrue(
          stat.getColumnCount() >= 3, "Column count should be at least 3 (id, user, ts)");

      log.info(
          "Collected stats for table with nested columns: columnCount={}", stat.getColumnCount());
    }
  }

  @Test
  public void testPartitionStatsLatestCommitOnly() throws Exception {
    final String tableName = "db.test_latest_commit";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);

      // Insert into same partition multiple times
      long timestamp = System.currentTimeMillis() / 1000;
      populateTable(ops, tableName, 1, 0, timestamp); // First commit
      Thread.sleep(100); // Small delay to ensure different commit timestamps
      populateTable(ops, tableName, 1, 0, timestamp); // Second commit (same partition)
      Thread.sleep(100);
      populateTable(ops, tableName, 1, 0, timestamp); // Third commit (same partition)

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Only one stats record (latest commit per partition)
      Assertions.assertEquals(
          1,
          partitionStats.size(),
          "Should have only 1 stats record (latest commit for the partition)");

      // Compare with partition events (which have all commits)
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);

      Assertions.assertEquals(
          3, partitionEvents.size(), "Partition events should have 3 records (all commits)");

      log.info(
          "Verified latest commit only: {} partition stats vs {} partition events",
          partitionStats.size(),
          partitionEvents.size());
    }
  }

  @Test
  public void testPartitionStatsFullAppIntegration() throws Exception {
    final String tableName = "db.test_full_integration_stats";
    final int numInserts = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      prepareTable(ops, tableName, true);
      populateTable(ops, tableName, numInserts);

      // Action: Run full app (collects stats, commits, partition events, and partition stats)
      TableStatsCollectionSparkApp app =
          new TableStatsCollectionSparkApp("test-job", null, tableName, otelEmitter);
      app.runInner(ops);

      // Verify: All four types collected
      IcebergTableStats tableStats = ops.collectTableStats(tableName);
      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(tableName);
      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(tableName);
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      Assertions.assertNotNull(tableStats, "Table stats should be collected");
      Assertions.assertFalse(commitEvents.isEmpty(), "Commit events should be collected");
      Assertions.assertFalse(partitionEvents.isEmpty(), "Partition events should be collected");
      Assertions.assertFalse(partitionStats.isEmpty(), "Partition stats should be collected");

      // Verify: Partition stats count is less than or equal to partition events
      // (stats have latest commit only, events have all commits)
      Assertions.assertTrue(
          partitionStats.size() <= partitionEvents.size(),
          "Partition stats should have fewer or equal records than partition events");

      log.info(
          "Full app integration: table stats={}, commits={}, partition events={}, partition stats={}",
          tableStats != null,
          commitEvents.size(),
          partitionEvents.size(),
          partitionStats.size());
    }
  }

  @Test
  public void testPartitionStatsEmptyTable() throws Exception {
    final String tableName = "db.test_stats_empty_table";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with no data
      prepareTable(ops, tableName, true);

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Empty list (no data = no stats)
      Assertions.assertTrue(
          partitionStats.isEmpty(), "Empty table should return empty partition stats");

      log.info("Empty table handled correctly for partition stats");
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

  @Test
  public void testPartitionStatsColumnLevelMetricsPopulated() throws Exception {
    final String tableName = "db.test_column_metrics";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with multiple data types
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (id INT, name STRING, age INT, score DOUBLE, ts TIMESTAMP) "
                      + "USING iceberg PARTITIONED BY (days(ts))",
                  tableName))
          .show();

      // Insert data with intentional nulls and varied values
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES "
                      + "(1, 'Alice', 25, 95.5, timestamp('2024-01-01')), "
                      + "(2, NULL, 30, 87.2, timestamp('2024-01-01')), "
                      + "(3, 'Charlie', NULL, 92.0, timestamp('2024-01-01')), "
                      + "(4, 'Diana', 28, NULL, timestamp('2024-01-02')), "
                      + "(5, 'Eve', 35, 88.8, timestamp('2024-01-02'))",
                  tableName))
          .show();

      // Action: Collect partition stats
      List<com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats>
          partitionStats = ops.collectCommitEventTablePartitionStats(tableName);

      // Verify: Stats collected for both partitions
      Assertions.assertTrue(
          partitionStats.size() >= 2, "Should have stats for at least 2 partitions");

      // Verify: Each partition has column-level metrics populated
      for (com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats stat :
          partitionStats) {
        log.info("Verifying column metrics for partition: {}", stat.getPartitionData());

        // Check null count metrics are present and valid
        Assertions.assertNotNull(stat.getNullCount(), "Null count map should not be null");
        Assertions.assertFalse(stat.getNullCount().isEmpty(), "Null count map should not be empty");

        // Log all null count entries
        log.info("Null count entries:");
        for (ColumnData cd : stat.getNullCount()) {
          log.info(
              "  Column '{}' has null count: {}",
              cd.getColumnName(),
              ((ColumnData.LongColumnData) cd).getValue());
        }

        // Should have null count for ALL columns (id, name, age, score, ts)
        Assertions.assertTrue(
            stat.getNullCount().size() >= 5,
            String.format(
                "Should have null count for all 5 columns (id, name, age, score, ts), got %d",
                stat.getNullCount().size()));

        // At least one column should have null count > 0 (we inserted nulls)
        boolean hasNonZeroNullCounts =
            stat.getNullCount().stream()
                .anyMatch(cd -> ((ColumnData.LongColumnData) cd).getValue() > 0);
        Assertions.assertTrue(
            hasNonZeroNullCounts,
            "At least one column should have null count > 0 since we inserted null values");

        // Check NaN count is populated for all columns
        Assertions.assertNotNull(stat.getNanCount(), "NaN count map should not be null");
        log.info("NaN count entries: {}", stat.getNanCount().size());
        for (ColumnData cd : stat.getNanCount()) {
          log.info(
              "  Column '{}' has NaN count: {}",
              cd.getColumnName(),
              ((ColumnData.LongColumnData) cd).getValue());
        }

        // Check column size metrics
        Assertions.assertNotNull(stat.getColumnSizeInBytes(), "Column size map should not be null");
        Assertions.assertFalse(
            stat.getColumnSizeInBytes().isEmpty(), "Column size map should not be empty");

        // Check min/max value metrics exist
        Assertions.assertNotNull(stat.getMinValue(), "Min value map should not be null");
        Assertions.assertNotNull(stat.getMaxValue(), "Max value map should not be null");

        // Verify min/max have entries for non-null columns
        log.info(
            "Column metrics summary: nullCount={}, minValue={}, maxValue={}, columnSize={}",
            stat.getNullCount().size(),
            stat.getMinValue().size(),
            stat.getMaxValue().size(),
            stat.getColumnSizeInBytes().size());

        // Verify we have min/max values for at least one column
        Assertions.assertTrue(
            stat.getMinValue().size() > 0 || stat.getMaxValue().size() > 0,
            "Should have min or max values for at least one column");

        // Verify specific column metrics - column names are preserved correctly
        for (ColumnData cd : stat.getNullCount()) {
          Assertions.assertNotNull(cd.getColumnName(), "Column name should not be null");
          Assertions.assertFalse(cd.getColumnName().isEmpty(), "Column name should not be empty");

          // Verify the value is accessible and valid
          Long nullCount = ((ColumnData.LongColumnData) cd).getValue();
          Assertions.assertTrue(
              nullCount >= 0,
              String.format(
                  "Null count for column '%s' should be non-negative", cd.getColumnName()));
        }
      }

      log.info("✅ Column-level metrics validation passed for all partitions");
    }
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
