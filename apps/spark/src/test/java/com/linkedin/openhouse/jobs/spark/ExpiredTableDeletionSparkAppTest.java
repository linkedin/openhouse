package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ExpiredTableDeletionSparkAppTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  private final OptimizerServiceClient optimizerClient = Mockito.mock(OptimizerServiceClient.class);

  @Test
  public void testExpiredTableIsDroppedAndReportsSuccess() throws Exception {
    String tableName = "db.expired_table_deletion_expired";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      createTemporaryTable(ops, tableName, 1000L);

      runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.SUCCESS, request.getStatus());
      Assertions.assertEquals(
          UpdateOperationRequest.OperationTypeEnum.EXPIRED_TABLE_DELETION,
          request.getOperationType());
      Assertions.assertEquals("db", request.getDatabaseName());
      Assertions.assertEquals("expired_table_deletion_expired", request.getTableName());
      Assertions.assertThrows(NoSuchTableException.class, () -> ops.getTable(tableName));
    }
  }

  @Test
  public void testTableThatExpiresLaterIsKeptAndReportsFailure() throws Exception {
    String tableName = "db.expired_table_deletion_future";
    long expirationMillis = Instant.now().plus(Duration.ofDays(1)).toEpochMilli();
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      createTemporaryTable(ops, tableName, expirationMillis);

      runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
      Assertions.assertEquals(
          "Table " + tableName + " expires at " + Instant.ofEpochMilli(expirationMillis) + ".",
          request.getFailureReason());
      Assertions.assertDoesNotThrow(() -> ops.getTable(tableName));
    }
  }

  @Test
  public void testOrdinaryTableIsKeptAndReportsFailure() throws Exception {
    String tableName = "db.expired_table_deletion_ordinary";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      ops.spark().sql(String.format("CREATE TABLE %s (data string)", tableName));

      runApp(ops, tableName);

      UpdateOperationRequest request = reportedRequest(tableName);
      Assertions.assertEquals(UpdateOperationRequest.StatusEnum.FAILED, request.getStatus());
      Assertions.assertEquals(
          "Table " + tableName + " is not a temporary table.", request.getFailureReason());
      Assertions.assertDoesNotThrow(() -> ops.getTable(tableName));
    }
  }

  @Test
  public void testMissingTableReportsSuccess() throws Exception {
    String tableName = "db.expired_table_deletion_missing";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);

      runApp(ops, tableName);

      Assertions.assertEquals(
          UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest(tableName).getStatus());
    }
  }

  @Test
  public void testUnexpectedFailureIsReportedAndLaterTablesAreStillDeleted() {
    Operations ops = Mockito.mock(Operations.class);
    Mockito.when(ops.getTable("db.unreadable"))
        .thenThrow(new IllegalStateException("Catalog unavailable."));
    Table expiredTable = Mockito.mock(Table.class);
    Mockito.when(expiredTable.properties())
        .thenReturn(Collections.singletonMap("openhouse.leaseExpirationTime", "1000"));
    Mockito.when(ops.getTable("db.expired")).thenReturn(expiredTable);

    runApp(ops, "db.unreadable", "db.expired");

    UpdateOperationRequest unreadableTableRequest = reportedRequest("db.unreadable");
    Assertions.assertEquals(
        UpdateOperationRequest.StatusEnum.FAILED, unreadableTableRequest.getStatus());
    Assertions.assertEquals(
        "java.lang.IllegalStateException: Catalog unavailable.",
        unreadableTableRequest.getFailureReason());
    Assertions.assertEquals(
        UpdateOperationRequest.StatusEnum.SUCCESS, reportedRequest("db.expired").getStatus());
    Mockito.verify(ops).dropTable("db.expired");
  }

  private void runApp(Operations ops, String... tableNames) {
    List<String> tableNameList = Arrays.asList(tableNames);
    new ExpiredTableDeletionSparkApp(
            "test-job",
            Mockito.mock(StateManager.class),
            otelEmitter,
            tableNameList,
            tableNameList.stream()
                .map(ExpiredTableDeletionSparkAppTest::operationId)
                .collect(Collectors.toList()),
            optimizerClient)
        .runInner(ops);
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

  private static void createTemporaryTable(
      Operations ops, String tableName, long expirationMillis) {
    ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE %s (data string) TBLPROPERTIES ('openhouse.leaseExpirationTime'='%d')",
                tableName, expirationMillis));
  }
}
