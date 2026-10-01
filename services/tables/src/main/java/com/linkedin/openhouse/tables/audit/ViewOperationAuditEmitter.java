package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

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
@Slf4j
@Component
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewOperationAuditEmitter {

  private static final MetricsReporter METRICS_REPORTER =
      MetricsReporter.of(MetricsConstant.SERVICE_AUDIT);

  /**
   * Matches {@code ServiceAuditAspect.SESSION_ID}: the same inbound header, one source of truth.
   */
  private static final String SESSION_ID_HEADER = "session-id";

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
    auditSafely(
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
    auditSafely(
        baseBuilder(prepared, outcome, actingPrincipal, sourceDialect)
            .operationStatus(OperationStatus.UNKNOWN)
            .build());
  }

  /** Emits a FAILED event for a write rejected before (or without) a commit outcome. */
  public void emitFailed(
      PreparedViewOperation prepared, String actingPrincipal, String sourceDialect) {
    auditSafely(
        baseBuilder(prepared, null, actingPrincipal, sourceDialect)
            .operationStatus(OperationStatus.FAILED)
            .build());
  }

  /**
   * Delivers through the same safe-reporting shape {@code ServiceAuditAspect} uses for its own
   * audit call, so no emission (success/failed/unknown) can throw back into the service or the
   * caller: one attempt, no retry, never silent (a fixed line is logged and a metric incremented).
   *
   * <p>Deliberate redaction deviation from {@code ServiceAuditAspect}: that aspect logs its caught
   * exception; this event may carry {@code sql}/{@code schema}/CAS-token context (plan
   * &sect;10/D4), so neither the throwable, its message, nor a stacktrace is logged here &mdash;
   * only the safe enum status and the opaque correlation id are.
   */
  private void auditSafely(ViewAuditEvent event) {
    try {
      auditHandler.audit(event);
    } catch (Exception redactedCause) {
      log.error(
          "View operation audit emission failed; status={} sessionId={} (cause suppressed for"
              + " redaction)",
          event.getOperationStatus(),
          event.getSessionId());
      METRICS_REPORTER.count(MetricsConstant.FAILED_SERVICE_AUDIT);
    }
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
        .sessionId(currentSessionId())
        .viewUUID(viewUuid)
        .oldMetadataLocation(oldPointer)
        .newMetadataLocation(newPointer);
  }

  /**
   * Reads the same inbound {@code session-id} header {@code ServiceAuditAspect} does, from the
   * request-bound context on the same request thread the emitter is always called from
   * (synchronously, inside the controller&rarr;service call), for success, early-failure, and
   * UNKNOWN alike. No new header, no generated id, no injected dependency: a unit context with no
   * bound request simply yields null, never a failure.
   */
  private static String currentSessionId() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (attributes instanceof ServletRequestAttributes) {
      return ((ServletRequestAttributes) attributes).getRequest().getHeader(SESSION_ID_HEADER);
    }
    return null;
  }
}
