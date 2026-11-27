package com.linkedin.openhouse.jobs.spark;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ExpiredTableDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final String DATABASE = "db_expired_table_deletion";
  private static final String TEMPORARY_KEY = "openhouse.temporary";
  private static final String EXPIRATION_TIME_KEY = "openhouse.expirationTime";
  private static final Map<String, String> EXPIRED_TEMPORARY_PROPERTIES =
      ImmutableMap.of(TEMPORARY_KEY, "true", EXPIRATION_TIME_KEY, "2020-01-01T00:00:00Z");

  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  private final OptimizerServiceClient optimizerClient = Mockito.mock(OptimizerServiceClient.class);

  @Test
  public void testExpiredTableIsDroppedAndReportsSuccess() throws Exception {
    String tableName = DATABASE + ".expired";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(ops, tableName, EXPIRED_TEMPORARY_PROPERTIES);
      Assertions.assertTrue(listedTableNames(ops).contains("expired"));

      runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.SUCCESS, request.getStatus());
      Assertions.assertEquals(
          UpdateOperationRequest.OperationTypeEnum.EXPIRED_TABLE_DELETION,
          request.getOperationType());
      Assertions.assertEquals(DATABASE, request.getDatabaseName());
      Assertions.assertEquals("expired", request.getTableName());
      Assertions.assertNull(request.getFailureReason());
      Assertions.assertFalse(listedTableNames(ops).contains("expired"));
    }
  }

  @Test
  public void testTableConvertedToOrdinaryIsNotDropped() throws Exception {
    String tableName = DATABASE + ".converted";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(ops, tableName, EXPIRED_TEMPORARY_PROPERTIES);
      ops.getTable(tableName).updateProperties().remove(TEMPORARY_KEY).commit();

      runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
      Assertions.assertTrue(listedTableNames(ops).contains("converted"));
    }
  }

  @Test
  public void testSameNamedOrdinaryTableIsNotDropped() throws Exception {
    String tableName = DATABASE + ".recreated";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(ops, tableName, ImmutableMap.of());

      runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
      Assertions.assertTrue(listedTableNames(ops).contains("recreated"));
    }
  }

  @Test
  public void testUnexpiredTemporaryTableIsKeptAndReportsFailure() throws Exception {
    String tableName = DATABASE + ".unexpired";
    String expirationTime =
        DateTimeFormatter.ISO_INSTANT.format(
            Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.SECONDS));
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(
          ops,
          tableName,
          ImmutableMap.of(TEMPORARY_KEY, "true", EXPIRATION_TIME_KEY, expirationTime));

      runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
      Assertions.assertEquals(
          "Table " + tableName + " has not expired yet (expires at " + expirationTime + ").",
          request.getFailureReason());
      Assertions.assertTrue(listedTableNames(ops).contains("unexpired"));
    }
  }

  @Test
  public void testMissingTableReportsSuccess() throws Exception {
    String tableName = DATABASE + ".missing";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);

      runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
    }
  }

  @Test
  public void testFailedDropReportsFailure() {
    String tableName = DATABASE + ".unreachable";
    Operations ops = Mockito.mock(Operations.class);
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(EXPIRED_TEMPORARY_PROPERTIES);
    Mockito.when(ops.findTable(tableName)).thenReturn(Optional.of(table));
    Mockito.when(ops.dropTable(tableName))
        .thenThrow(new IllegalStateException("Catalog unavailable."));

    runApp(ops, tableName);

    UpdateOperationRequest request = reportedRequest(tableName);
    Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
    Assertions.assertEquals(
        "java.lang.IllegalStateException: Catalog unavailable.", request.getFailureReason());
  }

  private void runApp(Operations ops, String tableName) {
    new ExpiredTableDeletionSparkApp(
            "test-job",
            Mockito.mock(StateManager.class),
            otelEmitter,
            tableName,
            operationId(tableName),
            optimizerClient)
        .runInner(ops);
  }

  /** Creates the table in one commit, so an already expired table never exists without its flag. */
  private static void createTable(
      Operations ops, String tableName, Map<String, String> properties) {
    Transaction transaction =
        ops.createTransaction(
            tableName, new Schema(Types.NestedField.optional(1, "data", Types.StringType.get())));
    UpdateProperties update = transaction.updateProperties();
    properties.forEach(update::set);
    update.commit();
    transaction.commitTransaction();
  }

  private static List<String> listedTableNames(Operations ops) {
    return ops.spark().sql("SHOW TABLES IN " + DATABASE).collectAsList().stream()
        .map(row -> row.<String>getAs("tableName"))
        .collect(Collectors.toList());
  }

  private UpdateOperationRequest reportedRequest(String tableName) {
    ArgumentCaptor<UpdateOperationRequest> request =
        ArgumentCaptor.forClass(UpdateOperationRequest.class);
    Mockito.verify(optimizerClient)
        .updateOperation(ArgumentMatchers.eq(operationId(tableName)), request.capture());
    return request.getValue();
  }

  private static String operationId(String tableName) {
    return "operation-" + tableName;
  }
}
