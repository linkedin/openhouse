package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** Driver-side metrics for one table operation, independent of optimizer callback delivery. */
@Slf4j
final class BatchedOperationMetrics {
  enum ValidationStatus {
    SUCCESS,
    FAILED,
    NOT_RUN
  }

  private final OtelEmitter emitter;
  private final String fqtn;
  private final Attributes attributes;
  private final Attributes tableAttributes;
  private final long startNanos = System.nanoTime();

  BatchedOperationMetrics(
      Operations ops,
      OtelEmitter emitter,
      String jobId,
      UpdateOperationRequest.OperationTypeEnum operationType,
      String databaseName,
      String fqtn) {
    this.emitter = emitter;
    this.fqtn = fqtn;
    String clusterName =
        ops.spark()
            .conf()
            .get("spark.sql.catalog.openhouse.cluster", System.getenv("CLUSTER_NAME"));
    if (StringUtils.isBlank(clusterName)) {
      log.warn("Cluster is not configured; operation metrics will use cluster_name=unknown");
      clusterName = "unknown";
    }
    if (StringUtils.isBlank(jobId)) {
      log.warn("Job ID is not configured; operation metrics will use job_id=unknown");
    }
    // Keep table/operation UUIDs out of the new series; each table still records its own sample.
    attributes =
        Attributes.builder()
            .put(AppConstants.OPERATION_TYPE, operationType.name())
            .put(AppConstants.CLUSTER_NAME, clusterName)
            .put(AppConstants.DATABASE_NAME, databaseName)
            .put(AppConstants.JOB_ID, StringUtils.defaultIfBlank(jobId, "unknown"))
            .build();
    tableAttributes = attributes.toBuilder().put(AppConstants.TABLE_NAME, fqtn).build();
  }

  Attributes getTableAttributes() {
    return tableAttributes;
  }

  void complete(UpdateOperationRequest.StatusEnum status) {
    long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    Attributes terminalAttributes =
        attributes.toBuilder().put(AppConstants.STATUS, status.name()).build();
    count(AppConstants.OPERATION_TERMINAL_COUNT, 1, terminalAttributes);
    try {
      emitter.time(
          BaseSparkApp.METRICS_SCOPE,
          AppConstants.OPERATION_DURATION,
          durationMillis,
          terminalAttributes);
    } catch (RuntimeException e) {
      log.error("Failed to emit operation duration: fqtn={}", fqtn, e);
    }
    log.info(
        "Operation completed: fqtn={} attributes={} status={} durationMs={}",
        fqtn,
        attributes,
        status,
        durationMillis);
  }

  void recordDeletion(Operations.FileDeletionMetrics deletion, ValidationStatus validationStatus) {
    count(AppConstants.RECLAIMED_FILE_COUNT, deletion.getDeletedFiles(), attributes);
    count(AppConstants.RECLAIMED_FILE_BYTES, deletion.getDeletedBytes(), attributes);
    count(
        AppConstants.RECLAIMED_FILE_SIZE_UNKNOWN_COUNT, deletion.getUnknownFileSizes(), attributes);
    count(
        AppConstants.POST_RUN_VALIDATION_COUNT,
        1,
        attributes.toBuilder().put(AppConstants.STATUS, validationStatus.name()).build());
    log.info(
        "Operation execution details: fqtn={} attributes={} reclaimedFiles={} reclaimedBytes={} unknownFileSizes={} validation={}",
        fqtn,
        attributes,
        deletion.getDeletedFiles(),
        deletion.getDeletedBytes(),
        deletion.getUnknownFileSizes(),
        validationStatus);
  }

  void count(String metric, long value, Attributes metricAttributes) {
    try {
      emitter.count(BaseSparkApp.METRICS_SCOPE, metric, value, metricAttributes);
    } catch (RuntimeException e) {
      log.error("Failed to emit operation metric: metric={} fqtn={}", metric, fqtn, e);
    }
  }
}
