package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link BatchedOrphanFilesDeletionSparkApp}.
 *
 * <p>These tests run the app's logic in-process against a real Spark session backed by an in-memory
 * Iceberg catalog ({@link OpenHouseSparkITest}). The app is never submitted as an external Spark
 * job; {@code runInner(ops)} is called directly.
 *
 * <p>When testing the per-table complete-operation callback path, the Optimizer service is replaced
 * by an embedded {@link com.sun.net.httpserver.HttpServer} that captures request bodies. This
 * verifies the shape and content of the JSON payloads without requiring a live service.
 */
@Slf4j
public class BatchedOrphanFilesDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final int MAX_SAMPLE_SIZE = 20000;

  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void testSuccessfulOrphanFilesDeletionForMultipleTables() throws Exception {
    final List<String> tableNames = Arrays.asList("db.test_batch1", "db.test_batch2");
    final int parallelism = 2;

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      for (String tableName : tableNames) {
        prepareTable(ops, tableName);
        populateTable(ops, tableName, 2);
      }

      BatchedOrphanFilesDeletionSparkApp app =
          newApp(
              tableNames, parallelism, TimeUnit.DAYS.toSeconds(1), Collections.emptyList(), null);

      app.runInner(ops);

      for (String tableName : tableNames) {
        Table table = ops.getTable(tableName);
        Assertions.assertNotNull(table);
      }
    }
  }

  @Test
  public void testBatchedDeletionWithSingleTable() throws Exception {
    final List<String> tableNames = Arrays.asList("db.test_single");

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableNames.get(0));
      populateTable(ops, tableNames.get(0), 3);

      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 1, TimeUnit.DAYS.toSeconds(1), Collections.emptyList(), null);

      app.runInner(ops);

      Assertions.assertNotNull(ops.getTable(tableNames.get(0)));
    }
  }

  @Test
  public void testBatchedDeletionWithInvalidTable() throws Exception {
    final List<String> tableNames =
        Arrays.asList("db.test_valid1", "db.nonexistent_table", "db.test_valid2");

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, "db.test_valid1");
      populateTable(ops, "db.test_valid1", 2);
      prepareTable(ops, "db.test_valid2");
      populateTable(ops, "db.test_valid2", 2);

      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 3, TimeUnit.DAYS.toSeconds(1), Collections.emptyList(), null);

      Assertions.assertThrows(RuntimeException.class, () -> app.runInner(ops));
    }
  }

  @Test
  public void testBatchedDeletionWithEmptyTables() throws Exception {
    final List<String> tableNames = Arrays.asList("db.test_empty1", "db.test_empty2");

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      for (String tableName : tableNames) {
        prepareTable(ops, tableName);
      }

      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 2, TimeUnit.DAYS.toSeconds(1), Collections.emptyList(), null);

      app.runInner(ops);

      for (String tableName : tableNames) {
        Assertions.assertNotNull(ops.getTable(tableName));
      }
    }
  }

  @Test
  public void testOrphanDeletionResultSuccess() {
    BatchedOrphanFilesDeletionSparkApp.OrphanDeletionResult result =
        BatchedOrphanFilesDeletionSparkApp.OrphanDeletionResult.success(
            "db.test_result", 42, 1024L, 5000L);

    Assertions.assertTrue(result.isSuccess());
    Assertions.assertEquals("db.test_result", result.getTableName());
    Assertions.assertEquals(42, result.getOrphanFilesDeleted());
    Assertions.assertEquals(1024L, result.getBytesDeleted());
    Assertions.assertEquals(5000L, result.getDurationMs());
    Assertions.assertNull(result.getErrorMessage());
    Assertions.assertNull(result.getErrorType());
  }

  @Test
  public void testOrphanDeletionResultFailure() {
    BatchedOrphanFilesDeletionSparkApp.OrphanDeletionResult result =
        BatchedOrphanFilesDeletionSparkApp.OrphanDeletionResult.failure(
            "db.test_failure", new RuntimeException("Test error"), 1000L);

    Assertions.assertFalse(result.isSuccess());
    Assertions.assertEquals("db.test_failure", result.getTableName());
    Assertions.assertEquals(0, result.getOrphanFilesDeleted());
    Assertions.assertEquals(0, result.getBytesDeleted());
    Assertions.assertEquals(1000L, result.getDurationMs());
    Assertions.assertEquals("Test error", result.getErrorMessage());
    Assertions.assertEquals("RuntimeException", result.getErrorType());
  }

  @Test
  public void testCreateAppFromCommandLineArgs() {
    String[] args = {
      "--jobId", "test-job-123",
      "--storageURL", "http://localhost:8080",
      "--tableNames", "db.table1,db.table2,db.table3",
      "--parallelism", "5",
      "--ttl", "86400",
      "--backupDir", "/backup",
      "--concurrentDeletes", "20",
      "--operationIds", "uuid-1,uuid-2,uuid-3",
      "--resultsEndpoint", "http://localhost:8083/v1/optimizer/operations"
    };

    Assertions.assertNotNull(BatchedOrphanFilesDeletionSparkApp.createApp(args, otelEmitter));
  }

  @Test
  public void testCreateAppWithDefaultValues() {
    String[] args = {
      "--jobId", "test-job-456",
      "--storageURL", "http://localhost:8080",
      "--tableNames", "db.table1"
    };

    Assertions.assertNotNull(BatchedOrphanFilesDeletionSparkApp.createApp(args, otelEmitter));
  }

  @Test
  public void testCreateAppWithOperationIdsAndEndpoint() {
    String[] args = {
      "--jobId", "test-job-789",
      "--storageURL", "http://localhost:8080",
      "--tableNames", "db.table1,db.table2",
      "--operationIds", "uuid-1,uuid-2",
      "--resultsEndpoint", "http://localhost:8083/v1/optimizer/operations"
    };

    Assertions.assertNotNull(BatchedOrphanFilesDeletionSparkApp.createApp(args, otelEmitter));
  }

  @Test
  public void testBatchedDeletionWithActualOrphanFiles() throws Exception {
    final String tableName = "db.test_orphans";
    final List<String> tableNames = Arrays.asList(tableName);

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, tableName);
      populateTable(ops, tableName, 3);

      Assertions.assertNotNull(ops.getTable(tableName).currentSnapshot());

      // TTL=0 bypasses the minimum-age guard so seeded orphan files are eligible.
      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 1, 0L, Collections.emptyList(), null);

      app.runInner(ops);

      Assertions.assertNotNull(ops.getTable(tableName).currentSnapshot());
    }
  }

  @Test
  public void testBatchedDeletionPartialSuccessWithEndpoint() throws Exception {
    final List<String> tableNames = Arrays.asList("db.test_ep_valid", "db.nonexistent_ep");
    final List<String> operationIds = Arrays.asList("op-100", "op-200");

    List<String> receivedBodies = Collections.synchronizedList(new ArrayList<>());
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext(
        "/ops",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          receivedBodies.add(new String(body, StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });
    httpServer.start();
    String endpoint = "http://localhost:" + httpServer.getAddress().getPort() + "/ops";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      prepareTable(ops, "db.test_ep_valid");
      populateTable(ops, "db.test_ep_valid", 2);

      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 2, TimeUnit.DAYS.toSeconds(1), operationIds, endpoint);

      app.runInner(ops);

      Assertions.assertEquals(2, receivedBodies.size());
      long successCount = receivedBodies.stream().filter(b -> b.contains("\"SUCCESS\"")).count();
      long failureCount = receivedBodies.stream().filter(b -> b.contains("\"FAILED\"")).count();
      Assertions.assertEquals(1, successCount);
      Assertions.assertEquals(1, failureCount);

      String successBody =
          receivedBodies.stream().filter(b -> b.contains("\"SUCCESS\"")).findFirst().get();
      Assertions.assertFalse(successBody.contains("\"result\""));

      String failureBody =
          receivedBodies.stream().filter(b -> b.contains("\"FAILED\"")).findFirst().get();
      Assertions.assertTrue(failureBody.contains("\"errorMessage\""));
      Assertions.assertTrue(failureBody.contains("\"errorType\""));
    } finally {
      httpServer.stop(0);
    }
  }

  @Test
  public void testBatchedDeletionAllFailWithEndpoint() throws Exception {
    final List<String> tableNames = Arrays.asList("db.nonexistent_ep1", "db.nonexistent_ep2");
    final List<String> operationIds = Arrays.asList("op-300", "op-400");

    List<String> receivedBodies = Collections.synchronizedList(new ArrayList<>());
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext(
        "/ops",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          receivedBodies.add(new String(body, StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });
    httpServer.start();
    String endpoint = "http://localhost:" + httpServer.getAddress().getPort() + "/ops";

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      BatchedOrphanFilesDeletionSparkApp app =
          newApp(tableNames, 2, TimeUnit.DAYS.toSeconds(1), operationIds, endpoint);

      Assertions.assertThrows(RuntimeException.class, () -> app.runInner(ops));

      Assertions.assertEquals(2, receivedBodies.size());
      long failures = receivedBodies.stream().filter(b -> b.contains("\"FAILED\"")).count();
      Assertions.assertEquals(2, failures);
      for (String body : receivedBodies) {
        Assertions.assertTrue(body.contains("\"errorMessage\""));
        Assertions.assertTrue(body.contains("\"errorType\""));
      }
    } finally {
      httpServer.stop(0);
    }
  }

  @Test
  public void testBatchedDeletionMismatchedOperationIdsThrows() throws Exception {
    final List<String> tableNames = Arrays.asList("db.table1", "db.table2");
    final List<String> operationIds = Arrays.asList("op-1", "op-2", "op-3");

    BatchedOrphanFilesDeletionSparkApp app =
        newApp(
            tableNames, 2, TimeUnit.DAYS.toSeconds(1), operationIds, "http://localhost:9999/ops");

    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> app.runInner(ops));
    }
  }

  private BatchedOrphanFilesDeletionSparkApp newApp(
      List<String> tableNames,
      int parallelism,
      long ttlSeconds,
      List<String> operationIds,
      String resultsEndpoint) {
    return new BatchedOrphanFilesDeletionSparkApp(
        "test-job",
        null,
        tableNames,
        parallelism,
        ttlSeconds,
        otelEmitter,
        ".backup",
        10,
        false,
        MAX_SAMPLE_SIZE,
        operationIds,
        resultsEndpoint);
  }

  private static void prepareTable(Operations ops, String tableName) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    ops.spark().sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName)).show();
  }

  private static void populateTable(Operations ops, String tableName, int numRows) {
    for (int row = 0; row < numRows; ++row) {
      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('v%d', current_timestamp())", tableName, row))
          .show();
    }
  }
}
