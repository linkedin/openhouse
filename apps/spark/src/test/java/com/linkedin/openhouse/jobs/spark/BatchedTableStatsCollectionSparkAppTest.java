package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Local integration tests for {@link BatchedTableStatsCollectionSparkApp} against a real local
 * Spark session + catalog (via {@link OpenHouseSparkITest}). Exercises the multi-table job logic
 * end-to-end: concurrent per-table collection, per-table failure isolation, the all-fail contract,
 * and partition-level artifacts. The optimizer-service callback is skipped (null results endpoint)
 * so the tests stay offline; published artifacts are captured through a generic {@link
 * StatsCollectionSink} rather than by subclassing.
 */
@Slf4j
public class BatchedTableStatsCollectionSparkAppTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testBatchedStatsCollection_allTablesSucceed() throws Exception {
    final String t1 = "db.batched_stats_a";
    final String t2 = "db.batched_stats_b";
    final String t3 = "db.batched_stats_c";
    final int numInserts = 3;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      for (String t : Arrays.asList(t1, t2, t3)) {
        prepareTable(ops, t);
        populateTable(ops, t, numInserts);
      }

      CapturingSink sink = new CapturingSink();
      BatchedTableStatsCollectionSparkApp app = newApp(entries(t1, t2, t3), 2, sink);
      app.runInner(ops);

      Assertions.assertEquals(
          Set.of(t1, t2, t3), sink.publishedStats, "all three tables should publish stats");
      Assertions.assertEquals(Set.of(t1, t2, t3), sink.publishedCommitEvents);
    }
  }

  @Test
  public void testBatchedStatsCollection_perTableFailureIsolation() throws Exception {
    final String good1 = "db.batched_stats_good1";
    final String good2 = "db.batched_stats_good2";
    final String missing = "db.batched_stats_missing"; // never created -> worker fails

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      for (String t : Arrays.asList(good1, good2)) {
        prepareTable(ops, t);
        populateTable(ops, t, 2);
      }
      ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", missing)).show();

      CapturingSink sink = new CapturingSink();
      BatchedTableStatsCollectionSparkApp app = newApp(entries(good1, missing, good2), 2, sink);
      // One bad table must not abort the batch: >=1 success means runInner returns normally.
      Assertions.assertDoesNotThrow(() -> app.runInner(ops));

      Assertions.assertEquals(
          Set.of(good1, good2), sink.publishedStats, "only the healthy tables publish stats");
    }
  }

  @Test
  public void testBatchedStatsCollection_allFail_throws() throws Exception {
    final String missing = "db.batched_stats_all_missing";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", missing)).show();

      CapturingSink sink = new CapturingSink();
      BatchedTableStatsCollectionSparkApp app = newApp(entries(missing), 1, sink);
      // Whole batch failed -> runInner must surface it so the job exits non-zero.
      Assertions.assertThrows(RuntimeException.class, () -> app.runInner(ops));
      Assertions.assertTrue(sink.publishedStats.isEmpty());
    }
  }

  @Test
  public void testBatchedStatsCollection_partitionedTable_publishesPartitionArtifacts()
      throws Exception {
    final String partitioned = "db.batched_stats_partitioned";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, partitioned, true);
      populateTable(ops, partitioned, 3);

      CapturingSink sink = new CapturingSink();
      BatchedTableStatsCollectionSparkApp app = newApp(entries(partitioned), 1, sink);
      app.runInner(ops);

      Assertions.assertTrue(sink.publishedStats.contains(partitioned));
      Assertions.assertTrue(
          sink.publishedPartitionStats.contains(partitioned),
          "partitioned table should publish partition stats");
    }
  }

  private List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries(String... fqtns) {
    return Arrays.stream(fqtns)
        .map(
            fqtn -> {
              String[] parts = fqtn.split("\\.", 2);
              return BatchedTableStatsCollectionSparkApp.BatchEntry.builder()
                  .fqtn(fqtn)
                  .operationId("op-" + parts[1])
                  .tableUuid("uuid-" + parts[1])
                  .databaseName(parts[0])
                  .tableName(parts[1])
                  .build();
            })
        .collect(java.util.stream.Collectors.toList());
  }

  /**
   * Build the real app wired to a capturing sink, with a null results endpoint so the
   * optimizer-service callback is skipped and the test stays offline.
   */
  private BatchedTableStatsCollectionSparkApp newApp(
      List<BatchedTableStatsCollectionSparkApp.BatchEntry> entries,
      int driverParallelism,
      StatsCollectionSink sink) {
    return new BatchedTableStatsCollectionSparkApp(
        "test-job", null, otelEmitter, entries, null, driverParallelism, sink);
  }

  /**
   * Generic capturing {@link StatsCollectionSink}: records which tables reached each publish call
   * (thread-safe, workers run concurrently). Exercises the app end-to-end through its real sink
   * seam rather than by subclassing to override a logger.
   */
  private static final class CapturingSink implements StatsCollectionSink {
    final Set<String> publishedStats = ConcurrentHashMap.newKeySet();
    final Set<String> publishedCommitEvents = ConcurrentHashMap.newKeySet();
    final Set<String> publishedPartitionStats = ConcurrentHashMap.newKeySet();

    @Override
    public void publishStats(String fqtn, IcebergTableStats icebergTableStats) {
      publishedStats.add(fqtn);
    }

    @Override
    public void publishCommitEvents(String fqtn, List<CommitEventTable> commitEvents) {
      publishedCommitEvents.add(fqtn);
    }

    @Override
    public void publishPartitionStats(
        String fqtn, List<CommitEventTablePartitionStats> partitionStats) {
      publishedPartitionStats.add(fqtn);
    }

    @Override
    public void publishPartitionEvents(
        String fqtn, List<CommitEventTablePartitions> partitionEvents) {
      // not asserted on
    }
  }

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
  }

  private static void populateTable(Operations ops, String tableName, int numRows) {
    long timestampSeconds = System.currentTimeMillis() / 1000;
    for (int row = 0; row < numRows; ++row) {
      ops.spark()
          .sql(
              String.format(
                  "INSERT INTO %s VALUES ('v%d', CAST(from_unixtime(%d) AS timestamp))",
                  tableName, row, timestampSeconds))
          .show();
    }
  }
}
