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

  private OperationStatus operationStatus;

  /** The view's identity, from the prepared capture or the committed outcome. */
  private String viewUUID;

  /** The pointer in effect before this operation; null on create. */
  private String oldMetadataLocation;

  /** The pointer published by this operation; null on drop and on an unacknowledged commit. */
  private String newMetadataLocation;
}
