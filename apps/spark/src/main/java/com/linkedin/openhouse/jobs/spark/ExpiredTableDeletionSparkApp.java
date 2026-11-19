package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import com.linkedin.openhouse.optimizer.model.TemporaryTable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Deletes temporary tables whose expiration time has passed. The Optimizer scheduler passes the
 * tables and their operation IDs as parallel lists, and the app reports the result of each
 * operation to the Optimizer service.
 *
 * <p>Example invocation:
 *
 * <pre>{@code
 * com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionSparkApp \
 *   --tableNames db.t1,db.t2 \
 *   --operationIds op-uuid-1,op-uuid-2 \
 *   --resultsEndpoint http://optimizer.svc:8080
 * }</pre>
 */
@Slf4j
public class ExpiredTableDeletionSparkApp extends BaseSparkApp {

  private final List<String> tableNames;
  private final List<String> operationIds;
  private final OptimizerServiceClient optimizerClient;

  public ExpiredTableDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      OtelEmitter otelEmitter,
      List<String> tableNames,
      List<String> operationIds,
      OptimizerServiceClient optimizerClient) {
    super(jobId, stateManager, otelEmitter);
    this.tableNames = tableNames;
    this.operationIds = operationIds;
    this.optimizerClient = optimizerClient;
  }

  @Override
  protected void runInner(Operations ops) {
    IntStream.range(0, tableNames.size())
        .forEach(index -> deleteAndReport(ops, tableNames.get(index), operationIds.get(index)));
  }

  private void deleteAndReport(Operations ops, String tableName, String operationId) {
    Optional<String> failureReason;
    try {
      failureReason = deleteIfExpired(ops, tableName);
    } catch (RuntimeException e) {
      log.error("Deleting table {} failed with an unexpected exception.", tableName, e);
      failureReason = Optional.of(e.toString());
    }
    reportResult(tableName, operationId, failureReason);
  }

  /**
   * Drops the table if its expiration time has passed and returns the reason the operation failed,
   * or empty when the table no longer exists. The app reloads the table to read its expiration time
   * because the Optimizer's copy of the table properties may be stale. A table whose expiration
   * time has not passed always fails, because a successful operation tells the Optimizer that the
   * table is deleted.
   */
  private Optional<String> deleteIfExpired(Operations ops, String tableName) {
    Table table;
    try {
      table = ops.getTable(tableName);
    } catch (NoSuchTableException e) {
      return Optional.empty();
    }
    Optional<Instant> expirationTime = TemporaryTable.expirationTime(table.properties());
    if (!expirationTime.isPresent()) {
      return Optional.of("Table " + tableName + " is not a temporary table.");
    }
    if (expirationTime.get().isAfter(Instant.now())) {
      return Optional.of("Table " + tableName + " expires at " + expirationTime.get() + ".");
    }
    log.info("Table {} expired at {} and is being dropped.", tableName, expirationTime.get());
    ops.dropTable(tableName);
    return Optional.empty();
  }

  private void reportResult(String tableName, String operationId, Optional<String> failureReason) {
    String[] databaseAndTable = tableName.split("\\.", 2);
    UpdateOperationRequest request =
        new UpdateOperationRequest()
            .operationId(operationId)
            .status(
                failureReason.isPresent()
                    ? UpdateOperationRequest.StatusEnum.FAILED
                    : UpdateOperationRequest.StatusEnum.SUCCESS)
            .databaseName(databaseAndTable[0])
            .tableName(databaseAndTable[1])
            .operationType(UpdateOperationRequest.OperationTypeEnum.EXPIRED_TABLE_DELETION)
            .failureReason(failureReason.orElse(null));
    if (!optimizerClient.updateOperation(operationId, request).isPresent()) {
      log.error(
          "The app could not report the result of operation {} for table {}.",
          operationId,
          tableName);
    }
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Collections.singletonList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static ExpiredTableDeletionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    CommandLine cmdLine =
        createCommandLine(
            args,
            Arrays.asList(
                new Option(null, "tableNames", true, "Comma-separated fully qualified table names"),
                new Option(
                    null,
                    "operationIds",
                    true,
                    "Comma-separated operation IDs, parallel to tableNames"),
                new Option(null, "resultsEndpoint", true, "Base URL of the Optimizer service")));
    List<String> tableNames = requireListOption(cmdLine, "tableNames");
    List<String> operationIds = requireListOption(cmdLine, "operationIds");
    if (tableNames.size() != operationIds.size()) {
      throw new IllegalArgumentException(
          String.format(
              "--tableNames and --operationIds must list the same number of entries, but they list %d and %d.",
              tableNames.size(), operationIds.size()));
    }
    Optional<String> unqualifiedTableName =
        tableNames.stream().filter(tableName -> !tableName.matches("[^.]+\\..+")).findFirst();
    if (unqualifiedTableName.isPresent()) {
      throw new IllegalArgumentException(
          "--tableNames entries must have the form db.table, but "
              + unqualifiedTableName.get()
              + " does not.");
    }
    return new ExpiredTableDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        otelEmitter,
        tableNames,
        operationIds,
        new OptimizerServiceClient(requireOption(cmdLine, "resultsEndpoint")));
  }

  private static List<String> requireListOption(CommandLine cmdLine, String name) {
    return Arrays.stream(requireOption(cmdLine, name).split(","))
        .map(String::trim)
        .collect(Collectors.toList());
  }

  private static String requireOption(CommandLine cmdLine, String name) {
    return Optional.ofNullable(cmdLine.getOptionValue(name))
        .filter(value -> !value.isEmpty())
        .orElseThrow(() -> new IllegalArgumentException("--" + name + " is required."));
  }
}
