package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Service-boundary operation-audit emitter. Emitted directly from {@code ViewsServiceImpl}, which
 * holds the pre-write context a handler aspect could not recover (a DELETE's UUID, or the pointer
 * in effect before a REPLACE) without a forbidden second read.
 *
 * <p>The service populates the {@link ViewAuditEvent} only from context actually reached; this
 * class adds the status and correlation id and delivers it.
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

  /**
   * A deployment supplies its own {@link AuditHandler} for {@link ViewAuditEvent}; otherwise view
   * operation events go to {@link DummyViewAuditHandler}. Following {@code
   * tables.api.ApiConfig#readBridgeConfigResolver}, the default arrives through {@link
   * ObjectProvider} rather than a competing OSS {@code @Component}, so a deployment bean cannot
   * collide with it and the override does not depend on bean-name matching.
   */
  @Autowired
  public ViewOperationAuditEmitter(ObjectProvider<AuditHandler<ViewAuditEvent>> viewAuditHandler) {
    this(viewAuditHandler.getIfAvailable(DummyViewAuditHandler::new));
  }

  /** Direct construction against an explicit handler, outside a Spring context. */
  public ViewOperationAuditEmitter(AuditHandler<ViewAuditEvent> viewAuditHandler) {
    this.auditHandler = viewAuditHandler;
  }

  /**
   * Completes {@code event} with its terminal status and the current inbound {@code session-id},
   * then attempts delivery once. The caller supplies every other field from context it actually
   * reached; nothing is fabricated here. Delivery cannot throw back into the service: a fixed line
   * is logged and a metric incremented instead, with no retry.
   *
   * <p>Deliberate deviation from {@code ServiceAuditAspect}, which logs its caught exception: a
   * sink exception may carry arbitrary sensitive text, so neither the throwable, its message, nor a
   * stacktrace is logged; only the status and the opaque correlation id are.
   */
  public void emit(ViewAuditEvent event, OperationStatus status) {
    ViewAuditEvent completed =
        event.toBuilder().operationStatus(status).sessionId(currentSessionId()).build();
    try {
      auditHandler.audit(completed);
    } catch (Exception redactedCause) {
      log.error(
          "View operation audit emission failed; status={} sessionId={} (cause suppressed for"
              + " redaction)",
          completed.getOperationStatus(),
          completed.getSessionId());
      METRICS_REPORTER.count(MetricsConstant.FAILED_SERVICE_AUDIT);
    }
  }

  /**
   * Reads the same inbound {@code session-id} header {@code ServiceAuditAspect} does, from the
   * request-bound context on the same request thread the emitter is always called from
   * (synchronously, inside the controller-to-service call), for success, early-failure, and UNKNOWN
   * alike. No new header, no generated id, no injected dependency: a unit context with no bound
   * request simply yields null, never a failure.
   */
  private static String currentSessionId() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (attributes instanceof ServletRequestAttributes) {
      return ((ServletRequestAttributes) attributes).getRequest().getHeader(SESSION_ID_HEADER);
    }
    return null;
  }
}
