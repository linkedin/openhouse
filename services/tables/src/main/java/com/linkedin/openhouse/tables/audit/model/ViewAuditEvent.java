package com.linkedin.openhouse.tables.audit.model;

import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Data model for auditing each view create/replace/drop operation, the view counterpart of {@link
 * TableAuditEvent}.
 *
 * <p>Built at the API handler boundary from the request, the handler's response and the thrown
 * failure only. It carries no view definition, SQL, schema or properties (the request-level audit
 * records the submitted request), and never a {@code Throwable}, cause or stacktrace.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ViewAuditEvent extends BaseAuditEvent {

  /** When the handler was entered; audit time, not the view's creation or modification time. */
  private Instant eventTimestamp;

  /** The serving cluster's configured name, not the response's {@code clusterId}. */
  private String clusterName;

  /** As requested: the request body's identifier, or the method argument for a drop. */
  private String databaseName;

  private String viewName;

  /** The authenticated principal. */
  private String user;

  /** The requested operation on failure; on success what the handler's response reports. */
  private ViewOperationType operationType;

  private OperationStatus operationStatus;

  /**
   * The metadata pointer in the successful create/replace response, which may equal the request's
   * base pointer for a no-op replace; null on failure and on drop.
   */
  private String newMetadataLocation;

  /**
   * Null on success. On failure, the name of a {@code ViewErrorCode}, or {@code ACCESS_DENIED} for
   * an authorization denial, which has no view error code. {@code COMMIT_STATE_UNKNOWN} means the
   * commit outcome is uncertain: the operation was reported FAILED, not proven uncommitted.
   */
  private String failureCode;

  /** Null on success. On failure, the reason phrase of the HTTP status the failure maps to. */
  private String failureMessage;
}
