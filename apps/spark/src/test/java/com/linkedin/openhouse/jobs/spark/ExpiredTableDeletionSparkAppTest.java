package com.linkedin.openhouse.jobs.spark;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionSparkApp.AuditRecord;
import com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionSparkApp.DeletionOutcome;
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
  private static final String UUID_KEY = "openhouse.tableUUID";
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
      String tableUuid = ops.getTable(tableName).properties().get(UUID_KEY);
      Instant beforeRun = Instant.now();

      AuditRecord record = runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.SUCCESS, request.getStatus());
      Assertions.assertEquals(
          UpdateOperationRequest.OperationTypeEnum.EXPIRED_TABLE_DELETION,
          request.getOperationType());
      Assertions.assertEquals(DATABASE, request.getDatabaseName());
      Assertions.assertEquals("expired", request.getTableName());
      Assertions.assertNull(request.getFailureReason());
      Assertions.assertFalse(listedTableNames(ops).contains("expired"));
      Assertions.assertEquals(DeletionOutcome.DROPPED, record.getOutcome());
      Assertions.assertEquals(operationId(tableName), record.getOperationId());
      Assertions.assertEquals(Optional.of("test-job"), record.getJobId());
      Assertions.assertNotNull(tableUuid);
      Assertions.assertEquals(Optional.of(tableUuid), record.getTableUuid());
      Assertions.assertEquals(DATABASE, record.getDatabaseName());
      Assertions.assertEquals("expired", record.getTableName());
      Assertions.assertEquals(Optional.of("2020-01-01T00:00:00Z"), record.getExpirationTime());
      Assertions.assertFalse(record.getCompletedAt().isBefore(beforeRun));
      Assertions.assertEquals(Optional.empty(), record.getFailureReason());
    }
  }

  @Test
  public void testTableConvertedToOrdinaryIsNotDropped() throws Exception {
    String tableName = DATABASE + ".converted";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(ops, tableName, EXPIRED_TEMPORARY_PROPERTIES);
      ops.getTable(tableName).updateProperties().remove(TEMPORARY_KEY).commit();

      AuditRecord record = runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
      Assertions.assertTrue(listedTableNames(ops).contains("converted"));
      Assertions.assertEquals(DeletionOutcome.NOT_TEMPORARY, record.getOutcome());
    }
  }

  @Test
  public void testSameNamedOrdinaryTableIsNotDropped() throws Exception {
    String tableName = DATABASE + ".recreated";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createTable(ops, tableName, ImmutableMap.of());

      AuditRecord record = runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
      Assertions.assertTrue(listedTableNames(ops).contains("recreated"));
      Assertions.assertEquals(DeletionOutcome.NOT_TEMPORARY, record.getOutcome());
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

      AuditRecord record = runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
      Assertions.assertEquals(
          "Table " + tableName + " has not expired yet (expires at " + expirationTime + ").",
          request.getFailureReason());
      Assertions.assertTrue(listedTableNames(ops).contains("unexpired"));
      Assertions.assertEquals(DeletionOutcome.NOT_EXPIRED, record.getOutcome());
      Assertions.assertEquals(Optional.of(expirationTime), record.getExpirationTime());
      Assertions.assertEquals(
          Optional.ofNullable(request.getFailureReason()), record.getFailureReason());
    }
  }

  @Test
  public void testMissingTableReportsSuccess() throws Exception {
    String tableName = DATABASE + ".missing";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);

      AuditRecord record = runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
      Assertions.assertEquals(DeletionOutcome.NOT_FOUND, record.getOutcome());
      Assertions.assertEquals(Optional.empty(), record.getTableUuid());
      Assertions.assertEquals(Optional.empty(), record.getExpirationTime());
    }
  }

  @Test
  public void testFailedDropReportsFailure() {
    String tableName = DATABASE + ".unreachable";
    Operations ops = Mockito.mock(Operations.class);
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties())
        .thenReturn(
            ImmutableMap.<String, String>builder()
                .putAll(EXPIRED_TEMPORARY_PROPERTIES)
                .put(UUID_KEY, "uuid-unreachable")
                .build());
    Mockito.when(ops.findTable(tableName)).thenReturn(Optional.of(table));
    Mockito.when(ops.dropTable(tableName))
        .thenThrow(new IllegalStateException("Catalog unavailable."));

    AuditRecord record = runApp(ops, tableName);

    UpdateOperationRequest request = reportedRequest(tableName);
    Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
    Assertions.assertEquals(
        "java.lang.IllegalStateException: Catalog unavailable.", request.getFailureReason());
    Assertions.assertEquals(DeletionOutcome.FAILED, record.getOutcome());
    Assertions.assertEquals(Optional.of("uuid-unreachable"), record.getTableUuid());
    Assertions.assertEquals(Optional.of(request.getFailureReason()), record.getFailureReason());
  }

  @Test
  public void testAuditRecordLogLineListsEveryField() {
    AuditRecord record =
        new AuditRecord(
            "op-1",
            Optional.of("job-1"),
            Optional.of("uuid-1"),
            "db",
            "t",
            Optional.of("2020-01-01T00:00:00Z"),
            Instant.parse("2020-01-02T03:04:05Z"),
            DeletionOutcome.DROPPED,
            Optional.empty());

    Assertions.assertEquals(
        "Expired table deletion audit: operationId=op-1 jobId=job-1 tableUuid=uuid-1"
            + " databaseName=db tableName=t expirationTime=2020-01-01T00:00:00Z"
            + " completedAt=2020-01-02T03:04:05Z outcome=DROPPED failureReason=none",
        record.toLogLine());
  }

  /** Runs the app and returns the one audit record it wrote. */
  private AuditRecord runApp(Operations ops, String tableName) {
    ExpiredTableDeletionSparkApp app =
        Mockito.spy(
            new ExpiredTableDeletionSparkApp(
                "test-job",
                Mockito.mock(StateManager.class),
                otelEmitter,
                tableName,
                operationId(tableName),
                optimizerClient));
    app.runInner(ops);
    ArgumentCaptor<AuditRecord> record = ArgumentCaptor.forClass(AuditRecord.class);
    Mockito.verify(app).recordAudit(record.capture());
    return record.getValue();
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
