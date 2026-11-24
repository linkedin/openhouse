package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ExpiredTableDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final String DATABASE = "db_expired_table_deletion";

  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  private final OptimizerServiceClient optimizerClient = Mockito.mock(OptimizerServiceClient.class);

  @Test
  public void testExpiredTableIsDroppedAndReportsSuccess() throws Exception {
    String tableName = DATABASE + ".expired";
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql("DROP TABLE IF EXISTS " + tableName);
      createExpiredTemporaryTable(ops, tableName);
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

  /**
   * Creates the table through the Iceberg catalog API. A Spark {@code CREATE TABLE} loads the table
   * after creating it, and that load fails because the tables service reports an expired temporary
   * table as not found.
   */
  private static void createExpiredTemporaryTable(Operations ops, String tableName) {
    Transaction transaction =
        ops.createTransaction(
            tableName, new Schema(Types.NestedField.optional(1, "data", Types.StringType.get())));
    transaction
        .updateProperties()
        .set("openhouse.temporary", "true")
        .set("openhouse.expirationTime", "2020-01-01T00:00:00Z")
        .commit();
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
