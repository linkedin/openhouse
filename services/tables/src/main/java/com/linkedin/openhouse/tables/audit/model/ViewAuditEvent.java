package com.linkedin.openhouse.tables.audit.model;

import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Data model for auditing each view create/replace/drop operation.
 *
 * <p>Populated only from context the service actually reached: the prepared capture (identity, old
 * pointer) and the committed outcome (new pointer), never from a second read or a fabricated value.
 * An ambiguous commit is recorded with {@link OperationStatus#UNKNOWN} and no new pointer. Carries
 * only safe fields: never a {@code Throwable}, a cause, or a stacktrace.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ViewAuditEvent extends BaseAuditEvent {

  private String databaseName;

  private String viewName;

  private String user;

  private String sourceDialect;

  /**
   * Correlation identifier joining this operation event to the request-level {@code
   * ServiceAuditEvent} for the same call: the same inbound {@code session-id} header, read once per
   * request. Internal only, never serialized to the wire; null when the header was absent or no
   * request context was bound (e.g. a unit test constructing this directly).
   */
  private String sessionId;

  private OperationStatus operationStatus;

  /** The view's identity, from the prepared capture or the committed outcome. */
  private String viewUUID;

  /** The pointer in effect before this operation; null on create. */
  private String oldMetadataLocation;

  /** The pointer published by this operation; null on drop and on an unacknowledged commit. */
  private String newMetadataLocation;
}
