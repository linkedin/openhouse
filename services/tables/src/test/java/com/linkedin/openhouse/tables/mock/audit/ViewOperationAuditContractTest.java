package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.gson.JsonParser;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.audit.ViewOperationAuditEmitter;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.mock.logging.Log4j2LogCapture;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Delivery contract of the operation-audit emitter: the service supplies the event it built from
 * the context it reached, and the emitter only completes it with the terminal status and inbound
 * session-id before one safe delivery attempt.
 */
public class ViewOperationAuditContractTest {

  private static final String SINK_SECRET = "SQL secret, schema secret, secret-token.metadata.json";

  @SuppressWarnings("unchecked")
  private final AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);

  private final ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);

  @AfterEach
  public void unbindRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

  @ParameterizedTest
  @EnumSource(OperationStatus.class)
  public void emitDeliversTheSuppliedContextCompletedOnlyWithItsStatus(OperationStatus status) {
    RequestContextHolder.resetRequestAttributes();
    ViewAuditEvent context = reachedContext();

    emitter.emit(context, status);

    ViewAuditEvent delivered = captureSingle();
    assertEquals(
        context.toBuilder().operationStatus(status).build().toJson(),
        delivered.toJson(),
        "every reached field is delivered unchanged; outside a request there is no session-id");
    assertNull(context.getOperationStatus(), "the caller's captured context is never mutated");
  }

  @Test
  public void emitCorrelatesWithTheInboundSessionIdOfTheBoundRequest() {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader("session-id", "correlation-1");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    emitter.emit(reachedContext(), OperationStatus.SUCCESS);

    assertEquals("correlation-1", captureSingle().getSessionId());
  }

  /** The serialized event is exactly the safe operation fields: no cause or stacktrace field. */
  @Test
  public void serializedEventCarriesOnlyTheOperationFields() {
    emitter.emit(reachedContext(), OperationStatus.UNKNOWN);

    assertEquals(
        Set.of(
            "databaseName",
            "viewName",
            "user",
            "sourceDialect",
            "sessionId",
            "operationStatus",
            "viewUUID",
            "oldMetadataLocation",
            "newMetadataLocation"),
        JsonParser.parseString(captureSingle().toJson()).getAsJsonObject().keySet());
  }

  /** One attempt, never thrown back into the service, and the sink's throwable is never logged. */
  @ParameterizedTest
  @EnumSource(OperationStatus.class)
  public void sinkFailureIsContainedWithAFixedDiagnostic(OperationStatus status) {
    doThrow(new IllegalStateException(SINK_SECRET, new RuntimeException(SINK_SECRET)))
        .when(auditHandler)
        .audit(any(ViewAuditEvent.class));

    String logs;
    try (Log4j2LogCapture capture = new Log4j2LogCapture()) {
      assertDoesNotThrow(() -> emitter.emit(reachedContext(), status));
      logs = capture.renderedEvents();
    }

    verify(auditHandler, times(1)).audit(any(ViewAuditEvent.class));
    assertTrue(logs.contains("View operation audit emission failed"), logs);
    assertFalse(logs.contains("secret"), logs);
    assertFalse(logs.contains(IllegalStateException.class.getName()), logs);
  }

  private static ViewAuditEvent reachedContext() {
    return ViewAuditEvent.builder()
        .databaseName(ViewModelConstants.DATABASE_ID)
        .viewName(ViewModelConstants.VIEW_ID)
        .user("alice")
        .sourceDialect(ViewModelConstants.SOURCE_DIALECT)
        .viewUUID("view-uuid")
        .oldMetadataLocation(ViewModelConstants.METADATA_LOCATION)
        .newMetadataLocation(
            "file:/warehouse/my_database/my_view/metadata/00002-committed.metadata.json")
        .build();
  }

  private ViewAuditEvent captureSingle() {
    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(auditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }
}
