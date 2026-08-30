package com.linkedin.openhouse.tables.audit.model;

import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import java.time.Instant;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for auditing each table operation. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class TableAuditEvent extends BaseAuditEvent {
  private Instant eventTimestamp;

  private String clusterName;

  private String databaseName;

  private String tableName;

  private String user;

  private OperationType operationType;

  private OperationStatus operationStatus;

  private String currentTableRoot;

  private String grantor;

  private String grantee;

  private String role;

  private Long currentSnapshotId;

  private Long currentSnapshotTimestampMs;

  /** Allowlisted subset of table properties at commit time, not the full property map. */
  private Map<String, String> auditedTableProperties;

  /**
   * Full Iceberg summary map of the snapshot the table's {@code main} ref points to after the
   * commit (the same snapshot as {@link #currentSnapshotId}), emitted verbatim. It carries {@code
   * operation} (append/overwrite/replace/delete), the engine/app identity ({@code spark.app.id} /
   * {@code trino_query_id}), {@code engine-version}/{@code iceberg-version}, and the core-computed
   * file-delta counts ({@code added-delete-files}, {@code added-data-files}, ...). Emitted
   * unaggregated so downstream queries derive operation, engine/app identity, and write-mode
   * signals without a lossy server-side classification: there is no per-commit field that cleanly
   * declares copy-on-write vs merge-on-read across engines (the table's {@code write.*.mode}
   * properties are intent that Trino/Flink ignore, and delete-file presence marks MOR but its
   * absence is not COW). {@code Snapshot.summary()} omits the operation (Iceberg parses it out into
   * a separate field), so it is merged back to mirror the on-disk summary. Null when the request
   * carries no {@code main} ref (e.g. branch-only commits), matching {@link #currentSnapshotId}.
   */
  private Map<String, String> snapshotSummary;

  /**
   * Raw {@code User-Agent} request header, verbatim (e.g. {@code openhouse-java-client/4.1.328},
   * {@code ReactorNetty/4.1.297}, {@code dev}). Stored unparsed so the client/runtime version can
   * be extracted at query time. Observability only; never gates a request.
   */
  private String clientUserAgent;
}
