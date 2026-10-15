package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.tables.audit.ViewOperationAuditEmitter;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.ViewAuditEvent;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ViewOperationAuditContractTest {

  @Test
  public void ambiguousViewCommitsEmitExactlyOneUnknownEventFromPreparedContext() {
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedRow());
    RuntimeException sensitiveCause =
        new RuntimeException("SQL secret, schema secret, file:/secret-token.metadata.json");

    emitter.emitUnknown(
        prepared, null, ACTING_PRINCIPAL, ViewModelConstants.SOURCE_DIALECT, sensitiveCause);

    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(auditHandler, times(1)).audit(event.capture());
    assertEquals(OperationStatus.UNKNOWN, event.getValue().getOperationStatus());
    assertEquals("view-uuid", event.getValue().getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, event.getValue().getOldMetadataLocation());
    assertNull(
        event.getValue().getNewMetadataLocation(),
        "Unknown publication has no trustworthy committed result/new pointer.");
    assertEquals(ACTING_PRINCIPAL, event.getValue().getUser());
    assertEquals(ViewModelConstants.SOURCE_DIALECT, event.getValue().getSourceDialect());
    AuditEventInspection.assertNoSensitiveProperties(
        event.getValue(),
        "SQL secret",
        "schema secret",
        "secret-token",
        sensitiveCause.getClass().getName());
  }

  @Test
  public void successfulCreateTakesUuidAndNewPointerFromTheCommittedOutcome() {
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);

    emitter.emitSuccess(
        PreparedViewOperation.observedAbsence(),
        outcome(COMMITTED_POINTER, "committed-create-uuid", true),
        ACTING_PRINCIPAL,
        ViewModelConstants.SOURCE_DIALECT);

    ViewAuditEvent event = captureSingle(auditHandler);
    assertEquals(OperationStatus.SUCCESS, event.getOperationStatus());
    assertEquals("committed-create-uuid", event.getViewUUID());
    assertNull(event.getOldMetadataLocation(), "A create has no prior pointer.");
    assertEquals(COMMITTED_POINTER, event.getNewMetadataLocation());
    assertEquals(ACTING_PRINCIPAL, event.getUser());
    assertEquals(ViewModelConstants.SOURCE_DIALECT, event.getSourceDialect());
  }

  @Test
  public void successfulReplaceTakesOldPointerFromCaptureAndNewPointerFromOutcome() {
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);

    emitter.emitSuccess(
        PreparedViewOperation.view(capturedRow()),
        outcome(COMMITTED_POINTER, "view-uuid", false),
        ACTING_PRINCIPAL,
        ViewModelConstants.SOURCE_DIALECT);

    ViewAuditEvent event = captureSingle(auditHandler);
    assertEquals(OperationStatus.SUCCESS, event.getOperationStatus());
    assertEquals("view-uuid", event.getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, event.getOldMetadataLocation());
    assertEquals(COMMITTED_POINTER, event.getNewMetadataLocation());
  }

  @Test
  public void successfulDropWithoutOutcomeUsesCapturedIdentityAndNoNewPointer() {
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);

    emitter.emitSuccess(PreparedViewOperation.view(capturedRow()), null, ACTING_PRINCIPAL, null);

    ViewAuditEvent event = captureSingle(auditHandler);
    assertEquals(OperationStatus.SUCCESS, event.getOperationStatus());
    assertEquals("view-uuid", event.getViewUUID());
    assertEquals(ViewModelConstants.METADATA_LOCATION, event.getOldMetadataLocation());
    assertNull(event.getNewMetadataLocation(), "A drop publishes no new pointer.");
  }

  /** Delivery failures are reported safely; no emission may throw back into the service. */
  @Test
  public void sinkFailuresNeverEscapeAnyEmission() {
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    org.mockito.Mockito.doThrow(new IllegalStateException("sink down"))
        .when(auditHandler)
        .audit(org.mockito.ArgumentMatchers.any(ViewAuditEvent.class));
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedRow());

    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () ->
            emitter.emitSuccess(
                prepared,
                outcome(COMMITTED_POINTER, "view-uuid", false),
                ACTING_PRINCIPAL,
                ViewModelConstants.SOURCE_DIALECT));
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> emitter.emitFailed(prepared, ACTING_PRINCIPAL, ViewModelConstants.SOURCE_DIALECT));
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () ->
            emitter.emitUnknown(
                prepared,
                null,
                ACTING_PRINCIPAL,
                ViewModelConstants.SOURCE_DIALECT,
                new RuntimeException("ambiguous")));
    verify(auditHandler, times(3)).audit(org.mockito.ArgumentMatchers.any(ViewAuditEvent.class));
  }

  /** Outside an HTTP request there is no inbound session-id, as for the request audit. */
  @Test
  public void eventsOutsideARequestHaveNoSessionId() {
    org.springframework.web.context.request.RequestContextHolder.resetRequestAttributes();
    @SuppressWarnings("unchecked")
    AuditHandler<ViewAuditEvent> auditHandler = mock(AuditHandler.class);
    ViewOperationAuditEmitter emitter = new ViewOperationAuditEmitter(auditHandler);

    emitter.emitFailed(
        PreparedViewOperation.view(capturedRow()),
        ACTING_PRINCIPAL,
        ViewModelConstants.SOURCE_DIALECT);

    ViewAuditEvent event = captureSingle(auditHandler);
    AuditEventInspection.assertHasProperty(event, "sessionId");
    assertNull(AuditEventInspection.properties(event).get("sessionId"));
  }

  private static final String ACTING_PRINCIPAL = "alice";
  private static final String COMMITTED_POINTER =
      "file:/warehouse/my_database/my_view/metadata/00002-committed.metadata.json";

  private static ViewCommitOutcome outcome(String pointer, String uuid, boolean created) {
    return ViewCommitOutcome.builder()
        .dto(
            ViewDto.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .viewId(ViewModelConstants.VIEW_ID)
                .metadataLocation(pointer)
                .viewVersion(pointer)
                .build())
        .committedViewUuid(uuid)
        .created(created)
        .build();
  }

  private static ViewAuditEvent captureSingle(AuditHandler<ViewAuditEvent> auditHandler) {
    ArgumentCaptor<ViewAuditEvent> event = ArgumentCaptor.forClass(ViewAuditEvent.class);
    verify(auditHandler, times(1)).audit(event.capture());
    return event.getValue();
  }

  private static HouseTable capturedRow() {
    return HouseTable.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .tableUUID("view-uuid")
        .tableLocation(ViewModelConstants.METADATA_LOCATION)
        .storageType("local")
        .entityType("VIEW")
        .build();
  }
}
