package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.linkedin.openhouse.tables.audit.ViewRequestPayloadRedactor;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
 * request the way the audit aspect calls it (synchronously, inside the request). Route grammar is
 * owned by {@code ViewRequestPayloadRedactorTest}; this covers one case per policy branch.
 */
public class ViewRequestAuditDiagnosticPolicyTest {

  private static final String VIEW_ROUTE = "/v1/databases/db/%76iews/v/";
  private static final String EVENT_JSON = "full-event-json-sentinel";
  private static final String SQL = "SELECT diagnostic_sql_marker FROM db.t";
  private static final String SESSION_ID = "diagnostic-session-1";
  private static final String SUMMARY =
      "Service audit summary: method=POST status=201 sessionId="
          + SESSION_ID
          + " (request details omitted)";

  @AfterEach
  public void clearRequestContext() {
    RequestContextHolder.resetRequestAttributes();
  }

  /**
   * A view request gets only the fixed summary: method, status and correlation id. The event is
   * never serialized for the log, and its definition-bearing payload is left as the same instance
   * for the custom-sink contract.
   */
  @Test
  public void viewRequestsLogOnlyAFixedSummary() {
    bindParsedRequest(VIEW_ROUTE);
    ServiceAuditEvent event = definitionBearingEvent(VIEW_ROUTE);
    JsonElement payload = event.getRequestPayload();
    String payloadBefore = payload.toString();

    assertEquals(SUMMARY, audit(handlerWith(viewPolicy()), event));

    verify(event, never()).toJson();
    assertSame(payload, event.getRequestPayload());
    assertEquals(payloadBefore, payload.toString(), "The event is not altered.");
  }

  /** A known non-view request keeps the existing full-event log, unchanged. */
  @Test
  public void nonViewRequestsKeepTheFullEventLog() {
    bindParsedRequest("/v1/databases/db/tables");
    ServiceAuditEvent event = definitionBearingEvent("/v1/databases/db/tables");

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
    ServiceAuditEvent event = definitionBearingEvent(VIEW_ROUTE);

    assertEquals(SUMMARY, audit(handlerWith(viewPolicy()), event));

    verify(event, never()).toJson();
  }

  /** A service with no payload policy (the default empty list) keeps the existing full log. */
  @Test
  public void withoutAnyPolicyTheFullEventIsLoggedAsBefore() {
    bindParsedRequest(VIEW_ROUTE);
    assertTrue(
        audit(new DummyServiceAuditHandler(), definitionBearingEvent(VIEW_ROUTE))
            .contains(EVENT_JSON));

    RequestContextHolder.resetRequestAttributes();
    assertTrue(
        audit(new DummyServiceAuditHandler(), definitionBearingEvent(VIEW_ROUTE))
            .contains(EVENT_JSON));
  }

  /**
   * A payload redactor that does not ask for diagnostic suppression, even when it applies to the
   * request, keeps the existing full log: the summary is opt-in, not triggered by any redactor.
   */
  @Test
  public void aRedactorThatDoesNotSuppressDiagnosticsKeepsTheFullEventLog() {
    List<ServiceAuditPayloadRedactor> appliesButDoesNotSuppress =
        Collections.singletonList(
            new ServiceAuditPayloadRedactor() {
              @Override
              public boolean appliesTo(HttpServletRequest request) {
                return true;
              }

              @Override
              public JsonElement redact(JsonElement requestPayload) {
                return requestPayload;
              }
            });
    bindParsedRequest(VIEW_ROUTE);
    assertTrue(
        audit(handlerWith(appliesButDoesNotSuppress), definitionBearingEvent(VIEW_ROUTE))
            .contains(EVENT_JSON));

    RequestContextHolder.resetRequestAttributes();
    assertTrue(
        audit(handlerWith(appliesButDoesNotSuppress), definitionBearingEvent(VIEW_ROUTE))
            .contains(EVENT_JSON));
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
   * A request audit event as the aspect builds it for a view write: the supplied definition kept.
   * {@code toJson} returns a sentinel so a log of the full event is detectable, and its invocation
   * is verifiable.
   */
  private static ServiceAuditEvent definitionBearingEvent(String rawRequestUri) {
    ServiceAuditEvent event =
        spy(
            ServiceAuditEvent.builder()
                .sessionId(SESSION_ID)
                .method(HttpMethod.POST)
                .uri(rawRequestUri)
                .requestPayload(
                    JsonParser.parseString(
                        "{\"representations\": [{\"type\": \"sql\", \"sql\": \"" + SQL + "\"}]}"))
                .statusCode(201)
                .build());
    doReturn(EVENT_JSON + " " + SQL).when(event).toJson();
    return event;
  }
}
