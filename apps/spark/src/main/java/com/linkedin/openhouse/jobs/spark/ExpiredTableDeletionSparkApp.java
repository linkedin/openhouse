package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.TemporaryTableProperties;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>Each run decides one {@link DeletionOutcome}, and both of the run's records come from it. The
 * run counts the outcome in {@link AppConstants#EXPIRED_TABLE_DELETION_COUNT}, records the time
 * from expiration to drop in {@link AppConstants#EXPIRED_TABLE_DELETION_LAG} when it drops the
 * table, and writes one {@link AuditRecord} line to the {@value #AUDIT_LOGGER_NAME} logger. The
 * Optimizer's operation history reports success both when the job dropped the table and when it
 * left the table in place, so the outcome is what tells the two apart.
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

  /** Logger that receives one {@link AuditRecord} line per deletion attempt. */
  public static final String AUDIT_LOGGER_NAME =
      "com.linkedin.openhouse.jobs.spark.ExpiredTableDeletionAudit";

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger(AUDIT_LOGGER_NAME);

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
    Optional<Map<String, String>> properties = Optional.empty();
    DeletionResult result;
    try {
      properties = ops.findTable(tableName).map(Table::properties);
      result = dropIfStillExpired(ops, properties);
    } catch (RuntimeException e) {
      log.error("Dropping table {} failed.", tableName, e);
      result = DeletionResult.of(DeletionOutcome.FAILED, Optional.of(e.toString()));
    }
    recordMetrics(result);
    recordAudit(auditRecord(properties, result));
    reportResult(result.getFailureReason());
  }

  private DeletionResult dropIfStillExpired(
      Operations ops, Optional<Map<String, String>> properties) {
    Instant now = Instant.now();
    if (!properties.isPresent()) {
      log.info("Table {} no longer exists, so there is nothing to drop.", tableName);
      return DeletionResult.of(DeletionOutcome.SKIPPED_MISSING, Optional.empty());
    } else if (!TemporaryTableProperties.isTemporary(properties.get())) {
      log.info("Skipped dropping table {} because it is no longer temporary.", tableName);
      return DeletionResult.of(DeletionOutcome.SKIPPED_NOT_TEMPORARY, Optional.empty());
    } else if (!TemporaryTableProperties.isExpired(properties.get(), now)) {
      String failureReason =
          String.format(
              "Table %s has not expired yet (expires at %s).",
              tableName,
              TemporaryTableProperties.expirationTime(properties.get())
                  .map(TemporaryTableProperties::formatExpirationTime)
                  .orElse("an unknown time"));
      log.warn(failureReason);
      return DeletionResult.of(DeletionOutcome.SKIPPED_NOT_EXPIRED, Optional.of(failureReason));
    } else if (ops.dropTable(tableName)) {
      log.info("Dropped expired table {}.", tableName);
      return new DeletionResult(
          DeletionOutcome.DELETED,
          Optional.empty(),
          TemporaryTableProperties.expirationTime(properties.get())
              .map(expirationTime -> Duration.between(expirationTime, now)));
    } else {
      log.info("Table {} no longer exists, so there is nothing to drop.", tableName);
      return DeletionResult.of(DeletionOutcome.SKIPPED_MISSING, Optional.empty());
    }
  }

  private AuditRecord auditRecord(Optional<Map<String, String>> properties, DeletionResult result) {
    String[] databaseAndTable = tableName.split("\\.", 2);
    return new AuditRecord(
        operationId,
        Optional.ofNullable(jobId),
        properties.map(values -> values.get(CatalogConstants.OPENHOUSE_UUID_KEY)),
        databaseAndTable[0],
        databaseAndTable[1],
        properties.map(values -> values.get(CatalogConstants.OPENHOUSE_EXPIRATION_TIME_KEY)),
        Instant.now(),
        result.getOutcome(),
        result.getFailureReason());
  }

  /**
   * Writes one audit line per attempt to the {@value #AUDIT_LOGGER_NAME} logger, so that "who
   * deleted my table, and when" can be answered from the job's logs even when the table is gone.
   */
  @VisibleForTesting
  protected void recordAudit(AuditRecord record) {
    AUDIT_LOG.info(record.toLogLine());
  }

  private void recordMetrics(DeletionResult result) {
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.EXPIRED_TABLE_DELETION_COUNT,
        1,
        Attributes.of(
            AttributeKey.stringKey(AppConstants.TABLE_NAME),
            tableName,
            AttributeKey.stringKey(AppConstants.OUTCOME),
            result.getOutcome().getAttributeValue()));
    result
        .getDeletionLag()
        .ifPresent(
            lag ->
                otelEmitter.time(
                    METRICS_SCOPE,
                    AppConstants.EXPIRED_TABLE_DELETION_LAG,
                    lag.toMillis(),
                    Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), tableName)));
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

  /**
   * The outcome of one run. Its attribute value tags {@link
   * AppConstants#EXPIRED_TABLE_DELETION_COUNT} and is the {@code outcome} field of the audit line.
   */
  public enum DeletionOutcome {
    /** The table was still temporary and expired, and the job dropped and purged it. */
    DELETED("deleted"),
    /** The table is no longer temporary, so the job left it in place. */
    SKIPPED_NOT_TEMPORARY("skipped_not_temporary"),
    /** The table is still temporary but has not expired, so the job left it in place. */
    SKIPPED_NOT_EXPIRED("skipped_not_expired"),
    /** The table did not exist when the job loaded or dropped it. */
    SKIPPED_MISSING("skipped_missing"),
    /** Loading or dropping the table threw an exception. */
    FAILED("failed");

    private final String attributeValue;

    DeletionOutcome(String attributeValue) {
      this.attributeValue = attributeValue;
    }

    public String getAttributeValue() {
      return attributeValue;
    }
  }

  /** What one run decided. The deletion lag is present only for {@link DeletionOutcome#DELETED}. */
  @Value
  private static class DeletionResult {
    DeletionOutcome outcome;
    Optional<String> failureReason;
    Optional<Duration> deletionLag;

    static DeletionResult of(DeletionOutcome outcome, Optional<String> failureReason) {
      return new DeletionResult(outcome, failureReason, Optional.empty());
    }
  }

  /**
   * The audit record of one deletion attempt. The table UUID and expiration time are the values the
   * job read from the table before acting, and are absent when the table did not exist.
   */
  @Value
  public static class AuditRecord {
    String operationId;
    Optional<String> jobId;
    Optional<String> tableUuid;
    String databaseName;
    String tableName;
    Optional<String> expirationTime;
    Instant completedAt;
    DeletionOutcome outcome;
    Optional<String> failureReason;

    /**
     * Formats the record as space-separated {@code key=value} pairs, with {@code none} for absent
     * values. The failure reason may contain spaces, so it comes last and runs to the end of the
     * line.
     */
    public String toLogLine() {
      return Stream.of(
              "operationId=" + operationId,
              "jobId=" + jobId.orElse("none"),
              "tableUuid=" + tableUuid.orElse("none"),
              "databaseName=" + databaseName,
              "tableName=" + tableName,
              "expirationTime=" + expirationTime.orElse("none"),
              "completedAt=" + completedAt,
              "outcome=" + outcome.getAttributeValue(),
              "failureReason=" + failureReason.orElse("none"))
          .collect(Collectors.joining(" ", "Expired table deletion audit: ", ""));
    }
  }
}
