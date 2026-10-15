package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.DummyServiceAuditHandler;
import com.linkedin.openhouse.common.audit.ServiceAuditPayloadRedactor;
import com.linkedin.openhouse.common.audit.model.ServiceAuditEvent;
import com.linkedin.openhouse.common.audit.model.ServiceName;
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpMethod;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * The built-in request-audit handler logs to ordinary diagnostics, so it must not log a view
 * request's definition, even though the request audit event itself keeps it for a custom sink. Uses
 * the actual {@link DummyServiceAuditHandler} with the actual view payload policy, bound to a
 * request the way the audit aspect calls it (synchronously, inside the request).
 */
public class ViewRequestAuditDiagnosticPolicyTest {

  private static final String EVENT_JSON = "full-event-json-sentinel";
  private static final String SQL = "SELECT diagnostic_sql_marker FROM db.t";
  private static final String SCHEMA = "diagnostic_schema_marker";
  private static final String PAGE_TOKEN = "diagnostic-page-token-marker";
  private static final String PRINCIPAL = "diagnostic-principal-marker";
  private static final String ACTION_TYPE = "diagnostic-action-type-marker";
  private static final String ERROR_TEXT = "diagnostic-error-text-marker";
  private static final String CAUSE_TEXT = "diagnostic-cause-marker";
  private static final String STACK_TEXT = "diagnostic-stacktrace-marker";
  private static final String SESSION_ID = "diagnostic-session-1";
  private static final String[] FORBIDDEN_IN_SUMMARY = {
    EVENT_JSON,
    "diagnostic_sql_marker",
    SCHEMA,
    PAGE_TOKEN,
    PRINCIPAL,
    ACTION_TYPE,
    ERROR_TEXT,
    CAUSE_TEXT,
    STACK_TEXT,
    "/v1/databases"
  };

  @AfterEach
  public void clearRequestContext() {
    RequestContextHolder.resetRequestAttributes();
  }

  /**
   * Every MVC-accepted view collection or item spelling gets only a fixed summary: method, status
   * and correlation id. The full event JSON, definition, tokens, URI, principal, action type,
   * response error text, cause and stacktrace never reach the log. The event is not altered.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/databases/db/views",
        "/v1/databases/db/views/",
        "/v1/databases/db/%76iews",
        "/v1/databases/db/views/v",
        "/v1/databases/db/%76iews/v/"
      })
  public void viewRequestsLogOnlyAFixedSummary(String rawRequestUri) {
    bindParsedRequest(rawRequestUri);
    ServiceAuditEvent event = definitionBearingEvent(rawRequestUri);
    String payloadBefore = event.getRequestPayload().toString();

    String logged = audit(handlerWith(viewPolicy()), event);

    verify(event, never()).toJson();
    assertTrue(logged.contains("POST"), logged);
    assertTrue(logged.contains("201"), logged);
    assertTrue(logged.contains(SESSION_ID), logged);
    assertNoForbidden(logged);
    assertEquals(payloadBefore, event.getRequestPayload().toString(), "The event is not altered.");
    assertTrue(event.getRequestPayload().toString().contains("diagnostic_sql_marker"));
    assertEquals(rawRequestUri + "?pageToken=[REDACTED]", event.getUri());
  }

  /** A known non-view request keeps the existing full-event log, unchanged. */
  @ParameterizedTest
  @ValueSource(strings = {"/v1/databases/db/tables", "/v1/databases/db/%74ables/t", "/jobs"})
  public void nonViewRequestsKeepTheFullEventLog(String rawRequestUri) {
    bindParsedRequest(rawRequestUri);
    ServiceAuditEvent event = definitionBearingEvent(rawRequestUri);

    String logged = audit(handlerWith(viewPolicy()), event);

    verify(event, times(1)).toJson();
    assertTrue(logged.contains(EVENT_JSON), logged);
  }

  /**
   * Called directly, outside any request, while a sensitive policy is installed, the route cannot
   * be established, so the handler fails closed to the summary.
   */
  @Test
  public void noRequestContextWithTheViewPolicyInstalledLogsOnlyTheSummary() {
    RequestContextHolder.resetRequestAttributes();
    ServiceAuditEvent event = definitionBearingEvent("/v1/databases/db/views");

    String logged = audit(handlerWith(viewPolicy()), event);

    verify(event, never()).toJson();
    assertTrue(logged.contains("201"), logged);
    assertNoForbidden(logged);
    assertTrue(event.getRequestPayload().toString().contains("diagnostic_sql_marker"));
  }

  /** A service with no payload policy (the default empty list) keeps the existing full log. */
  @Test
  public void withoutAnyPolicyTheFullEventIsLoggedAsBefore() {
    ServiceAuditEvent viewEvent = definitionBearingEvent("/v1/databases/db/views");
    bindParsedRequest("/v1/databases/db/views");
    assertTrue(audit(new DummyServiceAuditHandler(), viewEvent).contains(EVENT_JSON));

    RequestContextHolder.resetRequestAttributes();
    ServiceAuditEvent directEvent = definitionBearingEvent("/v1/databases/db/views");
    assertTrue(audit(new DummyServiceAuditHandler(), directEvent).contains(EVENT_JSON));
  }

  /**
   * A payload redactor that does not ask for diagnostic suppression, even when it applies to the
   * request, keeps the existing full log: the summary is opt-in, not triggered by any redactor.
   */
  @Test
  public void aRedactorThatDoesNotSuppressDiagnosticsKeepsTheFullEventLog() {
    ServiceAuditPayloadRedactor appliesButDoesNotSuppress =
        new ServiceAuditPayloadRedactor() {
          @Override
          public boolean appliesTo(HttpServletRequest request) {
            return true;
          }

          @Override
          public JsonElement redact(JsonElement requestPayload) {
            return requestPayload;
          }
        };
    bindParsedRequest("/v1/databases/db/views");
    ServiceAuditEvent viewEvent = definitionBearingEvent("/v1/databases/db/views");
    assertTrue(
        audit(handlerWith(Collections.singletonList(appliesButDoesNotSuppress)), viewEvent)
            .contains(EVENT_JSON));

    RequestContextHolder.resetRequestAttributes();
    ServiceAuditEvent directEvent = definitionBearingEvent("/v1/databases/db/views");
    assertTrue(
        audit(handlerWith(Collections.singletonList(appliesButDoesNotSuppress)), directEvent)
            .contains(EVENT_JSON));
  }

  /** The handler logs; it neither copies nor delivers the event elsewhere. */
  @Test
  public void theSummaryHandlerLeavesTheSameEventInstanceForItsCaller() {
    bindParsedRequest("/v1/databases/db/views");
    ServiceAuditEvent event = definitionBearingEvent("/v1/databases/db/views");
    JsonElement payload = event.getRequestPayload();

    audit(handlerWith(viewPolicy()), event);

    assertSame(payload, event.getRequestPayload());
  }

  private static List<ServiceAuditPayloadRedactor> viewPolicy() {
    return Collections.singletonList(new ViewRequestPayloadRedactor());
  }

  /** Wires the handler's optional payload-redactor list, as Spring would inject it. */
  private static DummyServiceAuditHandler handlerWith(List<ServiceAuditPayloadRedactor> policy) {
    DummyServiceAuditHandler handler = new DummyServiceAuditHandler();
    ReflectionTestUtils.setField(handler, null, policy, List.class);
    return handler;
  }

  /** Binds a request whose path MVC has parsed and cached, as on a dispatched request. */
  private static void bindParsedRequest(String rawRequestUri) {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", rawRequestUri);
    request.setRequestURI(rawRequestUri);
    request.addHeader("session-id", SESSION_ID);
    ServletRequestPathUtils.parseAndCache(request);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  private static String audit(DummyServiceAuditHandler handler, ServiceAuditEvent event) {
    try (Log4j2LogCapture logs = new Log4j2LogCapture()) {
      handler.audit(event);
      return logs.renderedEvents();
    }
  }

  /**
   * A request audit event as the aspect builds it for a view write: the supplied definition kept,
   * the CAS and page tokens already redacted. {@code toJson} returns a sentinel so a log of the
   * full event is detectable, and its invocation is verifiable.
   */
  private static ServiceAuditEvent definitionBearingEvent(String rawRequestUri) {
    JsonElement payload =
        JsonParser.parseString(
            "{\"schema\": \""
                + SCHEMA
                + "\", \"representations\": [{\"type\": \"sql\", \"sql\": \""
                + SQL
                + "\"}], \"baseMetadataLocation\": \"[REDACTED]\"}");
    ServiceAuditEvent event =
        spy(
            ServiceAuditEvent.builder()
                .startTimestamp(Instant.EPOCH)
                .endTimestamp(Instant.EPOCH)
                .sessionId(SESSION_ID)
                .clusterName("cluster")
                .serviceName(ServiceName.TABLES_SERVICE)
                .user(PRINCIPAL)
                .actionType(ACTION_TYPE)
                .method(HttpMethod.POST)
                .uri(rawRequestUri + "?pageToken=[REDACTED]")
                .requestPayload(payload)
                .statusCode(201)
                .responseErrorMessage(ERROR_TEXT + " " + PAGE_TOKEN)
                .cause(CAUSE_TEXT)
                .stacktrace(STACK_TEXT)
                .build());
    doReturn(EVENT_JSON + " " + SQL).when(event).toJson();
    return event;
  }

  private static void assertNoForbidden(String logged) {
    for (String forbidden : FORBIDDEN_IN_SUMMARY) {
      assertFalse(logged.contains(forbidden), forbidden + " in: " + logged);
    }
  }
}
