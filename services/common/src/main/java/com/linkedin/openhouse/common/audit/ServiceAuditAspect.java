package com.linkedin.openhouse.common.audit;

import static com.linkedin.openhouse.common.audit.CachingRequestBodyFilter.ATTRIBUTE_START_TIME;
import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.WebUtils;

/**
 * Aspect class to support request level auditing for all services including tables, jobs, and hts.
 * It enhances the ability of particular methods by adding logic of building and emitting audit
 * events.
 */
@Aspect
@Component
@Slf4j
public class ServiceAuditAspect {

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private AuditHandler<ServiceAuditEvent> serviceAuditHandler;

  /**
   * Redactors contributed by the running service, if any. Left empty when the service registers
   * none, in which case the payload is audited exactly as it was sent.
   */
  @Autowired(required = false)
  private List<ServiceAuditPayloadRedactor> payloadRedactors = Collections.emptyList();

  private static final MetricsReporter METRICS_REPORTER =
      MetricsReporter.of(MetricsConstant.SERVICE_AUDIT);

  private static final String SESSION_ID = "session-id";

  /**
   * Install the Around advice for all controller methods.
   *
   * @param point The controller method being enhanced
   * @return Result of the controller method
   * @throws Throwable Any exception during execution of the controller method
   */
  @Around("execution(@(io.swagger.v3.oas.annotations.responses.ApiResponses) * *(..))")
  protected Object auditSuccessfulRequests(ProceedingJoinPoint point) throws Throwable {
    Object result = null;
    try {
      result = point.proceed();
      try {
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (requestAttributes != null) {
          HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
          ResponseEntity<?> response = (ResponseEntity<?>) result;
          ServiceAuditEvent event =
              buildServiceAuditEvent(
                  (Instant) request.getAttribute(ATTRIBUTE_START_TIME),
                  Instant.now(),
                  request,
                  response,
                  false);
          serviceAuditHandler.audit(event);
        }
      } catch (Exception e) {
        log.error("Exception during auditing successful requests:\n", e);
        METRICS_REPORTER.count(MetricsConstant.FAILED_SERVICE_AUDIT);
      }
    } catch (Throwable t) {
      // Controller method failed. Just throw the error to be captured by {@link
      // com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler }
      // and {@link #auditFailedRequests(ProceedingJoinPoint)} will be invoked.
      throw t;
    }
    return result;
  }

  /**
   * Install the Around advice for all exception handler methods.
   *
   * @param point The exception handler method being enhanced
   * @return Result of the exception handler method
   * @throws Throwable Any exception during execution of the exception handler method
   */
  @Around("execution(* com.linkedin.openhouse.common.exception.handler.*.*(..))")
  protected Object auditFailedRequests(ProceedingJoinPoint point) throws Throwable {
    Object result = null;
    try {
      result = point.proceed();
      try {
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (requestAttributes != null) {
          HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
          ResponseEntity<ErrorResponseBody> response = (ResponseEntity<ErrorResponseBody>) result;
          ServiceAuditEvent event =
              buildServiceAuditEvent(
                  (Instant) request.getAttribute(ATTRIBUTE_START_TIME),
                  Instant.now(),
                  request,
                  response,
                  true);
          serviceAuditHandler.audit(event);
          return buildUpdatedResponseEntity(response);
        }
      } catch (Exception e) {
        log.error("Exception during auditing failed requests:\n", e);
        METRICS_REPORTER.count(MetricsConstant.FAILED_SERVICE_AUDIT);
      }
    } catch (Throwable t) {
      // If there's an error during exception handling, we just throw it because audit module should
      // not impact exception handling.
      throw t;
    }
    return result;
  }

  private ServiceAuditEvent buildServiceAuditEvent(
      Instant startTime,
      Instant endTime,
      HttpServletRequest request,
      ResponseEntity<?> response,
      boolean isFailed) {

    // Get request payload
    JsonElement requestPayload = null;
    try {
      ContentCachingRequestWrapper wrapper =
          WebUtils.getNativeRequest(request, ContentCachingRequestWrapper.class);
      if (wrapper != null) {
        String requestPayloadStr =
            new String(wrapper.getContentAsByteArray(), StandardCharsets.UTF_8);
        requestPayload = redactSensitiveValues(request, JsonParser.parseString(requestPayloadStr));
      }
    } catch (Exception e) {
      // Fail closed. A payload that could not be parsed or could not be redacted is dropped rather
      // than audited raw, so a broken redactor cannot leak the values it was meant to remove.
      requestPayload = null;
      log.error("Exception during parsing request payload:\n", e);
      METRICS_REPORTER.count(MetricsConstant.FAILED_PARSING_REQUEST_PAYLOAD);
    }
    // Get response error message
    String responseErrorMessage = null;
    String stacktrace = null;
    String cause = null;
    if (isFailed) {
      ErrorResponseBody errorResponseBody =
          ((ResponseEntity<ErrorResponseBody>) response).getBody();
      if (errorResponseBody != null) {
        responseErrorMessage = errorResponseBody.getMessage();
        stacktrace = errorResponseBody.getStacktrace();
        cause = errorResponseBody.getCause();
      }
    }
    // Get request uri and query string
    String uriAndQueryString =
        request.getQueryString() == null
            ? request.getRequestURI()
            : request.getRequestURI() + "?" + request.getQueryString();
    // Build ServiceAuditEvent
    return ServiceAuditEvent.builder()
        .startTimestamp(startTime)
        .endTimestamp(endTime)
        .sessionId(request.getHeader(SESSION_ID))
        .clusterName(clusterProperties.getClusterName())
        .serviceName(getServiceNameFromRequestURI(request.getRequestURI()))
        .user(extractAuthenticatedUserPrincipal())
        .method(HttpMethod.valueOf(request.getMethod()))
        .uri(uriAndQueryString)
        .requestPayload(requestPayload)
        .statusCode(response.getStatusCodeValue())
        .responseErrorMessage(responseErrorMessage)
        .stacktrace(stacktrace)
        .cause(cause)
        .build();
  }

  /**
   * Apply every registered redactor that claims this request. With no redactor registered — the
   * case for every service that has not contributed one — the parsed payload is returned unchanged,
   * so existing audit payloads are byte-identical.
   */
  private JsonElement redactSensitiveValues(
      HttpServletRequest request, JsonElement requestPayload) {
    if (requestPayload == null || payloadRedactors == null) {
      return requestPayload;
    }
    JsonElement redacted = requestPayload;
    for (ServiceAuditPayloadRedactor redactor : payloadRedactors) {
      if (redactor.appliesTo(request)) {
        redacted = redactor.redact(redacted);
      }
    }
    return redacted;
  }

  private ServiceName getServiceNameFromRequestURI(String uri) {
    if (uri.startsWith("/jobs")) {
      return ServiceName.JOBS_SERVICE;
    } else if (uri.startsWith("/hts")) {
      return ServiceName.HOUSETABLES_SERVICE;
    } else if (uri.startsWith("/databases") || uri.matches("/v[0-9]+/databases.*")) {
      return ServiceName.TABLES_SERVICE;
    }
    return ServiceName.UNRECOGNIZED;
  }

  /**
   * Build updated response for the client by excluding stacktrace
   *
   * @param response
   * @return ResponseEntity
   */
  private ResponseEntity<ErrorResponseBody> buildUpdatedResponseEntity(
      ResponseEntity<ErrorResponseBody> response) {
    ErrorResponseBody errorResponseBody = response.getBody();
    if (errorResponseBody != null) {
      ErrorResponseBody updatedErrorResponseBody =
          ErrorResponseBody.builder()
              .status(errorResponseBody.getStatus())
              .error(errorResponseBody.getError())
              .message(errorResponseBody.getMessage())
              .cause(errorResponseBody.getCause())
              .build();
      return new ResponseEntity<>(updatedErrorResponseBody, updatedErrorResponseBody.getStatus());
    }
    return response;
  }
}
