package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for TableStatsCollectorUtil methods.
 *
 * <p>Contains both unit tests (pure utility functions) and integration tests (methods requiring
 * Spark runtime). Integration tests directly invoke core methods like populateCommitEventTable() to
 * verify shadow JAR workarounds and DataFrame transformations in isolation.
 */
@Slf4j
public class TableStatsCollectorUtilTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testGetDatabaseName_withTwoPartName() {
    // Test: db.table format
    String result = TableStatsCollectorUtil.getDatabaseName("db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withThreePartName() {
    // Test: catalog.db.table format
    String result = TableStatsCollectorUtil.getDatabaseName("openhouse.db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withFourPartName() {
    // Test: Multiple namespace levels (rare but valid)
    String result = TableStatsCollectorUtil.getDatabaseName("catalog.schema.db.test_table");
    Assertions.assertEquals("db", result);
  }

  @Test
  public void testGetDatabaseName_withInvalidFormat() {
    // Test: Invalid format (no namespace)
    String result = TableStatsCollectorUtil.getDatabaseName("invalid_table");
    Assertions.assertNull(result);
  }

  @Test
  public void testGetDatabaseName_withNull() {
    // Test: Null input
    String result = TableStatsCollectorUtil.getDatabaseName(null);
    Assertions.assertNull(result);
  }

  @Test
  public void testGetDatabaseName_withEmptyString() {
    // Test: Empty string input
    String result = TableStatsCollectorUtil.getDatabaseName("");
    Assertions.assertNull(result);
  }

  // ==================== Integration Tests ====================
  // Tests below require Spark runtime and test core methods directly

  @Test
  public void testPopulateCommitEventTable_withMultipleCommits() throws Exception {
    final String tableName = "db.test_populate_commit_events";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with multiple commits
      ops.spark()
          .sql(
              String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", tableName))
          .show();

      for (int i = 0; i < numInserts; i++) {
        ops.spark()
            .sql(String.format("INSERT INTO %s VALUES ('v%d', current_timestamp())", tableName, i))
            .show();
      }

      // Load Iceberg table directly
      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable directly
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: Correct number of events
      Assertions.assertNotNull(commitEvents, "Commit events should not be null");
      Assertions.assertEquals(
          numInserts, commitEvents.size(), "Should collect one event per commit");

      // Verify: All required fields populated
      commitEvents.forEach(
          event -> {
            // Dataset fields
            Assertions.assertNotNull(event.getDataset(), "Dataset should not be null");
            Assertions.assertEquals("db", event.getDataset().getDatabaseName());
            Assertions.assertEquals(
                "test_populate_commit_events", event.getDataset().getTableName());

            // Commit metadata fields
            Assertions.assertNotNull(
                event.getCommitMetadata(), "CommitMetadata should not be null");
            Assertions.assertNotNull(event.getCommitMetadata().getCommitId());
            Assertions.assertNotNull(event.getCommitMetadata().getCommitTimestampMs());

            // Critical: Verify commitAppName is populated (was broken by shadow JAR)
            Assertions.assertNotNull(
                event.getCommitMetadata().getCommitAppName(),
                "commitAppName should be populated (shadow JAR fix verification)");

            // Critical: Verify commitAppId is populated (was broken by shadow JAR)
            Assertions.assertNotNull(
                event.getCommitMetadata().getCommitAppId(),
                "commitAppId should be populated (shadow JAR fix verification)");
          });

      log.info(
          "✅ populateCommitEventTable() verified: {} commit events collected", commitEvents.size());
    }
  }

  @Test
  public void testPopulateCommitEventTable_withNoCommits() throws Exception {
    final String tableName = "db.test_no_commits_util";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with no data (no commits/snapshots)
      ops.spark()
          .sql(
              String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", tableName))
          .show();

      // Load Iceberg table
      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable directly
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: Empty list (no snapshots = no commit events)
      Assertions.assertNotNull(commitEvents, "Should return empty list, not null");
      Assertions.assertTrue(
          commitEvents.isEmpty(), "Should have zero commit events for table with no snapshots");

      log.info("✅ populateCommitEventTable() correctly handles table with no commits");
    }
  }

  @Test
  public void testPopulateCommitEventTable_shadowJarWorkaround() throws Exception {
    final String tableName = "db.test_shadow_jar_workaround";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      ops.spark()
          .sql(
              String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", tableName))
          .show();

      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('test', current_timestamp())", tableName))
          .show();

      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable - exercises positional access workaround
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: The critical shadow JAR bug fix
      Assertions.assertFalse(commitEvents.isEmpty());

      CommitEventTable event = commitEvents.get(0);

      // These fields use runtime string concatenation to avoid shadow JAR relocation
      String appName = event.getCommitMetadata().getCommitAppName();
      String appId = event.getCommitMetadata().getCommitAppId();

      Assertions.assertNotNull(
          appName,
          "commitAppName must be populated (uses runtime string concatenation to avoid 'spark.app.name' relocation)");
      Assertions.assertNotNull(
          appId,
          "commitAppId must be populated (uses runtime string concatenation to avoid 'spark.app.id' relocation)");

      // Verify they don't contain the relocated prefix (would indicate bug)
      Assertions.assertFalse(
          appName.contains("openhouse.relocated"), "App name should not contain relocated prefix");
      Assertions.assertFalse(
          appId.contains("openhouse.relocated"), "App ID should not contain relocated prefix");

      log.info(
          "✅ Shadow JAR workaround verified: commitAppName={}, commitAppId={}", appName, appId);
    }
  }

  @Test
  public void testPopulateCommitEventTable_partitionedTable() throws Exception {
    final String tableName = "db.test_partitioned_commits";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create partitioned table
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, ts timestamp) USING iceberg PARTITIONED BY (days(ts))",
                  tableName))
          .show();

      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('test', current_timestamp())", tableName))
          .show();

      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable directly
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: Partition spec is captured correctly
      Assertions.assertFalse(commitEvents.isEmpty());

      String partitionSpec = commitEvents.get(0).getDataset().getPartitionSpec();
      Assertions.assertNotNull(partitionSpec, "Partition spec should be captured");
      Assertions.assertFalse(
          partitionSpec.contains("[]"), "Partitioned table should have non-empty partition spec");

      log.info("✅ populateCommitEventTable() captures partition spec: {}", partitionSpec);
    }
  }

  @Test
  public void testPopulateCommitEventTable_commitOrdering() throws Exception {
    final String tableName = "db.test_commit_ordering";
    final int numInserts = 4;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup: Create table with multiple commits
      ops.spark()
          .sql(
              String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", tableName))
          .show();

      for (int i = 0; i < numInserts; i++) {
        ops.spark()
            .sql(String.format("INSERT INTO %s VALUES ('v%d', current_timestamp())", tableName, i))
            .show();
        if (i < numInserts - 1) {
          Thread.sleep(10); // Small delay between commits
        }
      }

      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable directly
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: Events are ordered by commit timestamp (ascending)
      Assertions.assertEquals(numInserts, commitEvents.size());

      long previousTimestamp = 0;
      for (CommitEventTable event : commitEvents) {
        long currentTimestamp = event.getCommitMetadata().getCommitTimestampMs();
        Assertions.assertTrue(
            currentTimestamp >= previousTimestamp,
            "Commit events should be ordered by timestamp (ascending)");
        previousTimestamp = currentTimestamp;
      }

      log.info("✅ populateCommitEventTable() returns commits in chronological order");
    }
  }

  @Test
  public void testPopulateCommitEventTable_allFieldsPopulated() throws Exception {
    final String tableName = "db.test_all_fields";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      // Setup
      ops.spark()
          .sql(
              String.format("CREATE TABLE %s (data string, ts timestamp) USING iceberg", tableName))
          .show();

      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('test', current_timestamp())", tableName))
          .show();

      Table table = ops.getTable(tableName);

      // Action: Call populateCommitEventTable directly
      List<CommitEventTable> commitEvents =
          TableStatsCollectorUtil.populateCommitEventTable(table, ops.spark());

      // Verify: All fields are populated (exhaustive check)
      Assertions.assertFalse(commitEvents.isEmpty());

      CommitEventTable event = commitEvents.get(0);

      // Dataset fields (positional access from DataFrame)
      Assertions.assertNotNull(event.getDataset());
      Assertions.assertNotNull(event.getDataset().getDatabaseName());
      Assertions.assertNotNull(event.getDataset().getTableName());
      Assertions.assertNotNull(event.getDataset().getClusterName());
      Assertions.assertNotNull(event.getDataset().getTableMetadataLocation());
      Assertions.assertNotNull(event.getDataset().getPartitionSpec());

      // CommitMetadata fields (positional + runtime string building)
      Assertions.assertNotNull(event.getCommitMetadata());
      Assertions.assertNotNull(event.getCommitMetadata().getCommitId());
      Assertions.assertNotNull(event.getCommitMetadata().getCommitTimestampMs());
      Assertions.assertNotNull(event.getCommitMetadata().getCommitAppName());
      Assertions.assertNotNull(event.getCommitMetadata().getCommitAppId());
      Assertions.assertNotNull(event.getCommitMetadata().getCommitOperation());

      log.info("✅ All fields in CommitEventTable are populated correctly");
    }
  }
}
