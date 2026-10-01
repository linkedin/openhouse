package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * Service-boundary operation-audit emitter (plan &sect;10, R4). Emitted directly from {@code
 * ViewsServiceImpl} using the prepared capture and the commit outcome &mdash; never a handler
 * aspect, which cannot recover a DELETE's UUID/old pointer from a void response, or see the
 * pre-write pointer for REPLACE, without a forbidden second read.
 *
 * <p>Every field is populated only from context actually reached: an outcome supplies the new
 * pointer and the create UUID; the prepared capture supplies the prior identity/pointer when a row
 * was found. Nothing is fabricated for a field whose source was never reached (an ambiguous create
 * from observed absence has no identity and no new pointer, and none is invented here).
 */
@Component
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewOperationAuditEmitter {

  private final AuditHandler<ViewAuditEvent> auditHandler;

  @Autowired
  public ViewOperationAuditEmitter(AuditHandler<ViewAuditEvent> auditHandler) {
    this.auditHandler = auditHandler;
  }

  /** Emits a SUCCESS event. {@code outcome} is null only for a successful DROP. */
  public void emitSuccess(
      PreparedViewOperation prepared,
      ViewCommitOutcome outcome,
      String actingPrincipal,
      String sourceDialect) {
    auditHandler.audit(
        baseBuilder(prepared, outcome, actingPrincipal, sourceDialect)
            .operationStatus(OperationStatus.SUCCESS)
            .build());
  }

  /**
   * Emits exactly one UNKNOWN event for an ambiguous commit. {@code outcome} is always null: an
   * unacknowledged publish returns no committed result, so no new pointer is fabricated. The cause
   * is accepted only to document the caller's classification; it is never rendered into the event.
   */
  public void emitUnknown(
      PreparedViewOperation prepared,
      ViewCommitOutcome outcome,
      String actingPrincipal,
      String sourceDialect,
      Throwable cause) {
    auditHandler.audit(
        baseBuilder(prepared, outcome, actingPrincipal, sourceDialect)
            .operationStatus(OperationStatus.UNKNOWN)
            .build());
  }

  /** Emits a FAILED event for a write rejected before (or without) a commit outcome. */
  public void emitFailed(
      PreparedViewOperation prepared, String actingPrincipal, String sourceDialect) {
    auditHandler.audit(
        baseBuilder(prepared, null, actingPrincipal, sourceDialect)
            .operationStatus(OperationStatus.FAILED)
            .build());
  }

  private ViewAuditEvent.ViewAuditEventBuilder baseBuilder(
      PreparedViewOperation prepared,
      ViewCommitOutcome outcome,
      String actingPrincipal,
      String sourceDialect) {
    HouseTable capturedRow = prepared.getViewBaseRow().orElse(null);
    String databaseName =
        outcome != null ? outcome.getDto().getDatabaseId() : prepared.auditDatabaseId();
    String viewName = outcome != null ? outcome.getDto().getViewId() : prepared.auditViewId();
    String viewUuid =
        outcome != null
            ? outcome.getCommittedViewUuid()
            : capturedRow == null ? null : capturedRow.getTableUUID();
    String oldPointer = prepared.getCapturedTableLocation();
    String newPointer = outcome == null ? null : outcome.getDto().getMetadataLocation();

    return ViewAuditEvent.builder()
        .databaseName(databaseName)
        .viewName(viewName)
        .user(actingPrincipal)
        .sourceDialect(sourceDialect)
        .viewUUID(viewUuid)
        .oldMetadataLocation(oldPointer)
        .newMetadataLocation(newPointer);
  }
}
