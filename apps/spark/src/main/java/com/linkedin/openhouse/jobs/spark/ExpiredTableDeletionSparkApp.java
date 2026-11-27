package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.internal.catalog.TemporaryTableProperties;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.iceberg.Table;

/**
 * Drops and purges one expired temporary table, then reports the result to the Optimizer service.
 *
 * <p>The Optimizer's analyzer decides that the table has expired, but the table may have changed
 * since then: it may have been converted to an ordinary table, or dropped and replaced by an
 * ordinary table with the same name. The app therefore loads the table and drops it only when it is
 * still temporary and still expired. A table that is no longer temporary is left in place and the
 * operation reports success, because there is nothing left to delete. A table that no longer exists
 * counts as deleted, so a retried job succeeds.
 *
 * <p>A table that is still temporary but has not expired by this job's clock is also left in place,
 * and the operation reports failure. The analyzer stops scheduling a table once a success completes
 * at or after its expiration time, so a success here, stamped after the expiration by a clock that
 * runs behind, would leave the table in place forever. A failure makes the analyzer retry.
 *
 * <p>The scheduler passes {@code --tableNames} and {@code --operationIds} for every operation type.
 * For this job, each must hold exactly one entry.
 *
 * <p>Example invocation:
 *
 * <pre>{@code
 * com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionSparkApp \
 *   --tableNames db.t1 \
 *   --operationIds op-uuid-1 \
 *   --resultsEndpoint http://optimizer.svc:8080
 * }</pre>
 */
@Slf4j
public class ExpiredTableDeletionSparkApp extends BaseSparkApp {

  private final String tableName;
  private final String operationId;
  private final OptimizerServiceClient optimizerClient;

  public ExpiredTableDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      OtelEmitter otelEmitter,
      String tableName,
      String operationId,
      OptimizerServiceClient optimizerClient) {
    super(jobId, stateManager, otelEmitter);
    this.tableName = tableName;
    this.operationId = operationId;
    this.optimizerClient = optimizerClient;
  }

  @Override
  protected void runInner(Operations ops) {
    Optional<String> failureReason;
    try {
      failureReason = dropIfStillExpired(ops);
    } catch (RuntimeException e) {
      log.error("Dropping table {} failed.", tableName, e);
      failureReason = Optional.of(e.toString());
    }
    reportResult(failureReason);
  }

  /** Returns the failure reason when the table is still temporary but has not expired yet. */
  private Optional<String> dropIfStillExpired(Operations ops) {
    Optional<Map<String, String>> properties = ops.findTable(tableName).map(Table::properties);
    if (!properties.isPresent()) {
      log.info("Table {} no longer exists, so there is nothing to drop.", tableName);
    } else if (!TemporaryTableProperties.isTemporary(properties.get())) {
      log.info("Skipped dropping table {} because it is no longer temporary.", tableName);
    } else if (!TemporaryTableProperties.isExpired(properties.get(), Instant.now())) {
      String failureReason =
          String.format(
              "Table %s has not expired yet (expires at %s).",
              tableName,
              TemporaryTableProperties.expirationTime(properties.get())
                  .map(TemporaryTableProperties::formatExpirationTime)
                  .orElse("an unknown time"));
      log.warn(failureReason);
      return Optional.of(failureReason);
    } else if (ops.dropTable(tableName)) {
      log.info("Dropped expired table {}.", tableName);
    } else {
      log.info("Table {} no longer exists, so there is nothing to drop.", tableName);
    }
    return Optional.empty();
  }

  private void reportResult(Optional<String> failureReason) {
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
                new Option(null, "tableNames", true, "The fully qualified name of one table"),
                new Option(null, "operationIds", true, "The ID of the table's operation"),
                new Option(null, "resultsEndpoint", true, "Base URL of the Optimizer service")));
    String tableName = requireSingleEntry(cmdLine, "tableNames");
    if (!tableName.matches("[^.]+\\..+")) {
      throw new IllegalArgumentException(
          "--tableNames must have the form db.table, but " + tableName + " does not.");
    }
    return new ExpiredTableDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        otelEmitter,
        tableName,
        requireSingleEntry(cmdLine, "operationIds"),
        new OptimizerServiceClient(requireOption(cmdLine, "resultsEndpoint")));
  }

  private static String requireSingleEntry(CommandLine cmdLine, String name) {
    List<String> entries = Arrays.asList(requireOption(cmdLine, name).split(",", -1));
    if (entries.size() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Each job drops one table, so --%s must hold exactly one entry, but it holds %d.",
              name, entries.size()));
    }
    return entries.get(0).trim();
  }

  private static String requireOption(CommandLine cmdLine, String name) {
    return Optional.ofNullable(cmdLine.getOptionValue(name))
        .filter(value -> !value.isEmpty())
        .orElseThrow(() -> new IllegalArgumentException("--" + name + " is required."));
  }
}
