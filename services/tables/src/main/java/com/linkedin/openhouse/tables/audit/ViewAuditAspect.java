package com.linkedin.openhouse.tables.audit;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.audit.model.ViewOperationType;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

/**
 * Aspect that audits view create, replace and drop at the {@code ViewsApiHandler} boundary, like
 * {@code TableAuditAspect} does for tables. Reads and requests rejected before a handler is invoked
 * (binding failures, identifier mismatch) produce no operation event; the request-level audit still
 * records those.
 *
 * <p>The event is named from the request, timestamped before the handler runs, and completed with
 * the server cluster, the authenticated user and the outcome. A failure carries only a controlled
 * code and reason phrase, never the throwable, its message, cause or stack trace.
 *
 * <p>Unlike {@code TableAuditAspect}, delivery is isolated: a sink exception is neither reported as
 * a second FAILED event nor allowed to replace the handler's response or throwable. It is attempted
 * once, logged as a fixed line without the exception, and counted.
 *
 * <p>Registered unconditionally, unlike {@code ViewsServiceImpl}: it references only API, model and
 * audit types, and has nothing to advise when no {@code ViewsApiHandler} bean exists.
 */
@Slf4j
@Aspect
@Component
public class ViewAuditAspect {

  private static final MetricsReporter METRICS_REPORTER =
      MetricsReporter.of(MetricsConstant.SERVICE_AUDIT);

  private final ClusterProperties clusterProperties;

  private final AuditHandler<ViewAuditEvent> auditHandler;

  /**
   * A deployment supplies its own {@link AuditHandler} for {@link ViewAuditEvent}; otherwise view
   * operation events go to {@link DummyViewAuditHandler}. Following {@code
   * tables.api.ApiConfig#readBridgeConfigResolver}, the default arrives through {@link
   * ObjectProvider} rather than a competing OSS {@code @Component}, so a deployment bean cannot
   * collide with it and the override does not depend on bean-name matching.
   */
  public ViewAuditAspect(
      ClusterProperties clusterProperties,
      ObjectProvider<AuditHandler<ViewAuditEvent>> viewAuditHandler) {
    this.clusterProperties = clusterProperties;
    this.auditHandler = viewAuditHandler.getIfAvailable(DummyViewAuditHandler::new);
  }

  /** Install the Around advice for createView() in ViewsApiHandler. */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.ViewsApiHandler.createView(..)) "
          + "&& args(requestBody, *)")
  protected ApiResponse<GetViewResponseBody> auditCreateView(
      ProceedingJoinPoint point, CreateUpdateViewRequestBody requestBody) throws Throwable {
    return proceedAndAudit(
        point,
        namedEvent(requestBody.getDatabaseId(), requestBody.getViewId()),
        response -> ViewOperationType.CREATE,
        ViewOperationType.CREATE);
  }

  /**
   * Install the Around advice for updateView() in ViewsApiHandler. A returned 201 is authoritative
   * for CREATE and 200 for REPLACE, including a no-op replace. A failure only has the request, so
   * it reports the requested operation: CREATE for the initial base version, otherwise REPLACE.
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.ViewsApiHandler.updateView(..)) "
          + "&& args(requestBody, *)")
  protected ApiResponse<GetViewResponseBody> auditUpdateView(
      ProceedingJoinPoint point, CreateUpdateViewRequestBody requestBody) throws Throwable {
    return proceedAndAudit(
        point,
        namedEvent(requestBody.getDatabaseId(), requestBody.getViewId()),
        response ->
            response.getHttpStatus() == HttpStatus.CREATED
                ? ViewOperationType.CREATE
                : ViewOperationType.REPLACE,
        INITIAL_TABLE_VERSION.equals(requestBody.getBaseMetadataLocation())
            ? ViewOperationType.CREATE
            : ViewOperationType.REPLACE);
  }

  /** Install the Around advice for deleteView() in ViewsApiHandler. */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<Void> "
          + "com.linkedin.openhouse.tables.api.handler.ViewsApiHandler.deleteView(..)) "
          + "&& args(databaseId, viewId, *)")
  protected ApiResponse<Void> auditDeleteView(
      ProceedingJoinPoint point, String databaseId, String viewId) throws Throwable {
    return proceedAndAudit(
        point,
        namedEvent(databaseId, viewId),
        response -> ViewOperationType.DROP,
        ViewOperationType.DROP);
  }

  private static ViewAuditEvent namedEvent(String databaseId, String viewId) {
    return ViewAuditEvent.builder()
        .eventTimestamp(Instant.now())
        .databaseName(databaseId)
        .viewName(viewId)
        .build();
  }

  /**
   * Runs the handler once and audits its outcome. The handler's response or throwable is returned
   * or rethrown unchanged, and the success audit sits outside the handler's catch, so a delivery
   * failure can never turn an acknowledged operation into a FAILED event.
   */
  @SuppressWarnings("unchecked")
  private <T> ApiResponse<T> proceedAndAudit(
      ProceedingJoinPoint point,
      ViewAuditEvent event,
      Function<ApiResponse<T>, ViewOperationType> successType,
      ViewOperationType failureType)
      throws Throwable {
    ApiResponse<T> result;
    try {
      result = (ApiResponse<T>) point.proceed();
    } catch (Throwable t) {
      deliver(OperationStatus.FAILED, () -> failed(event, failureType, t));
      throw t;
    }
    deliver(OperationStatus.SUCCESS, () -> succeeded(event, successType.apply(result), result));
    return result;
  }

  private ViewAuditEvent succeeded(
      ViewAuditEvent event, ViewOperationType type, ApiResponse<?> response) {
    Object body = response.getResponseBody();
    return complete(event, type, OperationStatus.SUCCESS)
        .toBuilder()
        .newMetadataLocation(
            body instanceof GetViewResponseBody
                ? ((GetViewResponseBody) body).getMetadataLocation()
                : null)
        .build();
  }

  /**
   * Maps the thrown failure to a controlled code and the HTTP reason phrase of its status, as the
   * view exception handler maps it. The code is a {@link ViewErrorCode} name, or {@code
   * ACCESS_DENIED} for an authorization denial, which has no view error code. Because the API
   * handler has already translated an uncertain commit, it is reported as FAILED with {@code
   * COMMIT_STATE_UNKNOWN}; FAILED here means the handler threw, not that nothing was committed.
   */
  private ViewAuditEvent failed(ViewAuditEvent event, ViewOperationType type, Throwable failure) {
    String code;
    HttpStatus status;
    if (failure instanceof ViewApiException) {
      ViewErrorCode errorCode = ((ViewApiException) failure).getErrorCode();
      code = errorCode.name();
      status = errorCode.getHttpStatus();
    } else if (failure instanceof AuthorizationServiceException) {
      // Checked before AccessDeniedException, which it extends.
      code = ViewErrorCode.VIEW_SERVICE_UNAVAILABLE.name();
      status = HttpStatus.SERVICE_UNAVAILABLE;
    } else if (failure instanceof AccessDeniedException) {
      code = "ACCESS_DENIED";
      status = HttpStatus.FORBIDDEN;
    } else {
      code = ViewErrorCode.INTERNAL_VIEW_ERROR.name();
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    }
    return complete(event, type, OperationStatus.FAILED)
        .toBuilder()
        .failureCode(code)
        .failureMessage(status.getReasonPhrase())
        .build();
  }

  private ViewAuditEvent complete(
      ViewAuditEvent event, ViewOperationType type, OperationStatus status) {
    return event
        .toBuilder()
        .clusterName(clusterProperties.getClusterName())
        .user(extractAuthenticatedUserPrincipal())
        .operationType(type)
        .operationStatus(status)
        .build();
  }

  /**
   * One delivery attempt, no retry. Building the event is inside the guard too, so nothing here can
   * reach the caller. A sink exception may carry arbitrary sensitive text, so neither the
   * throwable, its message, nor a stack trace is logged; only the status is.
   */
  private void deliver(OperationStatus status, Supplier<ViewAuditEvent> event) {
    try {
      auditHandler.audit(event.get());
    } catch (Exception redactedCause) {
      log.error(
          "View operation audit emission failed; status={} (cause suppressed for redaction)",
          status);
      METRICS_REPORTER.count(MetricsConstant.FAILED_SERVICE_AUDIT);
    }
  }
}
