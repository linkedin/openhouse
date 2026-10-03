package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.tables.audit.ViewOperationAuditEmitter;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;

public class ViewsServiceImplTest {

  private static final String ACTING_PRINCIPAL = "alice";

  private ViewsFeatureGate featureGate;
  private DatabasesService databasesService;
  private OpenHouseInternalViewRepository viewRepository;
  private AuthorizationUtils authorizationUtils;
  private AuthorizationHandler authorizationHandler;
  private ViewAdmissionService admissionService;
  private ViewWritePrivilegeMapper privilegeMapper;
  private ViewOperationAuditEmitter viewOperationAuditEmitter;
  private ClusterProperties clusterProperties;
  private ViewsServiceImpl service;

  @BeforeEach
  public void setup() {
    featureGate = mock(ViewsFeatureGate.class);
    databasesService = mock(DatabasesService.class);
    viewRepository = mock(OpenHouseInternalViewRepository.class);
    authorizationHandler = mock(AuthorizationHandler.class);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(true);
    authorizationUtils = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorizationUtils, "authorizationHandler", authorizationHandler);
    admissionService = mock(ViewAdmissionService.class);
    privilegeMapper = mock(ViewWritePrivilegeMapper.class);
    viewOperationAuditEmitter = mock(ViewOperationAuditEmitter.class);
    clusterProperties = mock(ClusterProperties.class);
    when(clusterProperties.getClusterName()).thenReturn(ViewModelConstants.CLUSTER_ID);
    service = new ViewsServiceImpl();
    ReflectionTestUtils.setField(service, "featureGate", featureGate);
    ReflectionTestUtils.setField(service, "databasesService", databasesService);
    ReflectionTestUtils.setField(service, "viewRepository", viewRepository);
    ReflectionTestUtils.setField(service, "authorizationUtils", authorizationUtils);
    ReflectionTestUtils.setField(service, "admissionService", admissionService);
    ReflectionTestUtils.setField(service, "privilegeMapper", privilegeMapper);
    ReflectionTestUtils.setField(service, "viewOperationAuditEmitter", viewOperationAuditEmitter);
    ReflectionTestUtils.setField(service, "clusterProperties", clusterProperties);
  }

  @Test
  public void disabledGateShortCircuitsDatabaseProbesAuthorizationAndRepository() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);

    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.getView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL))
            .getErrorCode());
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.getAllViews(
                        ViewModelConstants.DATABASE_ID, null, 50, "viewId", ACTING_PRINCIPAL))
            .getErrorCode());
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.putView(
                        ViewModelConstants.createRequestWithoutBaseVersion(),
                        ACTING_PRINCIPAL,
                        true))
            .getErrorCode());
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.putView(
                        ViewModelConstants.createRequestWithInitialBaseVersion(),
                        ACTING_PRINCIPAL,
                        false))
            .getErrorCode());
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.deleteView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL))
            .getErrorCode());
    verifyNoInteractions(databasesService, viewRepository, authorizationHandler, admissionService);
  }

  @Test
  public void missingDatabaseFailsBeforePrepareOrAuthorization() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.DATABASE_NOT_FOUND, thrown.getErrorCode());
    verifyNoInteractions(viewRepository, authorizationHandler, admissionService);
  }

  @Test
  public void getViewPerformsNoAuthorizationAndPopulatesServerClusterId() {
    existingEnabledDatabase();
    ViewDto stored =
        ViewDto.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .viewId(ViewModelConstants.VIEW_ID)
            .metadataLocation(ViewModelConstants.METADATA_LOCATION)
            .build();
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(stored);
    when(clusterProperties.getClusterName()).thenReturn("server-cluster");

    ViewDto result =
        service.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    assertEquals("server-cluster", result.getClusterId());
    verifyNoInteractions(authorizationHandler);
  }

  @Test
  public void createAuthorizesAtDatabaseLevelBeforeAdmissionAndCommit() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    ViewCommitOutcome committed = createdOutcome();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenReturn(committed);
    Pair<ViewDto, Boolean> result =
        service.putView(
            ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true);

    assertEquals(committed.getDto(), result.getFirst());
    assertEquals(Boolean.TRUE, result.getSecond());
    InOrder order =
        inOrder(
            featureGate,
            databasesService,
            viewRepository,
            privilegeMapper,
            authorizationHandler,
            admissionService);
    order.verify(featureGate).isEnabled(ViewModelConstants.DATABASE_ID);
    order.verify(databasesService).getAllDatabases();
    order
        .verify(viewRepository)
        .prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    order.verify(privilegeMapper).forCreate();
    order.verify(authorizationHandler).checkAccessDecision(any(), any(DatabaseDto.class), any());
    order.verify(admissionService).admit(ViewModelConstants.createRequestWithoutBaseVersion());
    order
        .verify(viewRepository)
        .commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL);
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
  }

  @Test
  public void createAuditReceivesTheCommittedOutcomeCarryingTheEngineUuid() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    ViewCommitOutcome outcome = createdOutcome();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenReturn(outcome);

    Pair<ViewDto, Boolean> result =
        service.putView(
            ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true);

    assertEquals(outcome.getDto(), result.getFirst());
    assertEquals(Boolean.TRUE, result.getSecond());
    ArgumentCaptor<ViewCommitOutcome> audited = ArgumentCaptor.forClass(ViewCommitOutcome.class);
    verify(viewOperationAuditEmitter)
        .emitSuccess(
            org.mockito.ArgumentMatchers.same(absence),
            audited.capture(),
            org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL),
            org.mockito.ArgumentMatchers.eq(ViewModelConstants.SOURCE_DIALECT));
    assertEquals(COMMITTED_CREATE_UUID, audited.getValue().getCommittedViewUuid());
    assertEquals(
        ViewModelConstants.METADATA_LOCATION, audited.getValue().getDto().getMetadataLocation());
    verify(viewRepository, times(1))
        .prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(viewRepository, never()).findById(any(), any());
  }

  @Test
  public void deniedWriteDoesNotAdmitAllocateOrCommit() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(false);

    assertThrows(
        AccessDeniedException.class,
        () ->
            service.putView(
                ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    verify(admissionService, never()).admit(any());
    verify(viewRepository, never()).commitCreate(any(), any(), any());
  }

  @Test
  public void absentPutCreatesOnlyWithInitialVersionAndRejectsStaleTokensBeforeAdmission() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    ViewCommitOutcome committed = createdOutcome();
    when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithInitialBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenReturn(committed);

    Pair<ViewDto, Boolean> initialResult =
        service.putView(
            ViewModelConstants.createRequestWithInitialBaseVersion(), ACTING_PRINCIPAL, false);

    assertEquals(Boolean.TRUE, initialResult.getSecond());
    clearInvocations(admissionService, viewRepository);

    ViewApiException stale =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false));
    assertEquals(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, stale.getErrorCode());
    verify(admissionService, never()).admit(any());
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
  }

  @Test
  public void existingPutWithStaleBaseTokenRejectsBeforeAdmissionOrCommit() {
    existingEnabledDatabase();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(
            PreparedViewOperation.view(
                capturedViewRow()
                    .toBuilder()
                    .tableLocation("file:/warehouse/current.metadata.json")
                    .build()));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);

    ViewApiException stale =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false));

    assertEquals(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, stale.getErrorCode());
    assertDatabaseAuthorization(Privileges.UPDATE_TABLE_METADATA);
    verify(admissionService, never()).admit(any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
  }

  @Test
  public void replaceCommitFailureMapsToConcurrentModificationWithoutCreateOrDeleteSwitch() {
    existingEnabledDatabase();
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedViewRow());
    CommitFailedException source = new CommitFailedException("stale replace");
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(prepared);
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewRepository.commitReplace(
            ViewModelConstants.fullyPopulatedRequest(), prepared, ACTING_PRINCIPAL))
        .thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false));

    assertEquals(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, thrown.getErrorCode());
    assertSame(source, thrown.getCause());
    verify(viewRepository, times(1))
        .commitReplace(ViewModelConstants.fullyPopulatedRequest(), prepared, ACTING_PRINCIPAL);
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void missingDefaultNamespaceDatabaseFailsBeforePrepareOrAdmission() {
    existingEnabledDatabase();

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithInitialBaseVersion()
                        .toBuilder()
                        .defaultNamespace(Arrays.asList("missing_database"))
                        .build(),
                    ACTING_PRINCIPAL,
                    false));

    assertEquals(ViewErrorCode.DATABASE_NOT_FOUND, thrown.getErrorCode());
    verify(viewRepository, never()).prepareWrite(any(), any());
    verify(admissionService, never()).admit(any());
  }

  @Test
  public void existingDefaultNamespaceDatabaseAllowsCreateToProceed() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Arrays.asList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build(),
                DatabaseDto.builder().databaseId("dependency_database").build()));
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    ViewCommitOutcome committed = createdOutcome();
    com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody request =
        ViewModelConstants.createRequestWithInitialBaseVersion()
            .toBuilder()
            .defaultNamespace(Arrays.asList("dependency_database"))
            .build();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(request, absence, ACTING_PRINCIPAL)).thenReturn(committed);

    Pair<ViewDto, Boolean> result = service.putView(request, ACTING_PRINCIPAL, false);

    assertEquals(Boolean.TRUE, result.getSecond());
    assertEquals(committed.getDto(), result.getFirst());
    verify(admissionService).admit(request);
  }

  @Test
  public void absentOrWrongTypeReadAndDropReturnNoSuchViewWithoutWriteAuthorizationLeakage() {
    existingEnabledDatabase();
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(new ViewApiException(ViewErrorCode.NO_SUCH_VIEW, "No such view"));
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.observedAbsence());
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    assertEquals(
        ViewErrorCode.NO_SUCH_VIEW,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.getView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL))
            .getErrorCode());
    verifyNoInteractions(authorizationHandler);
    clearInvocations(viewRepository);
    assertEquals(
        ViewErrorCode.NO_SUCH_VIEW,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.deleteView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL))
            .getErrorCode());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void ambiguousDropIsOneAttemptUnknownWithCapturedOldPointerOnly() {
    existingEnabledDatabase();
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedViewRow());
    CommitStateUnknownException unknown =
        new CommitStateUnknownException(new RuntimeException("ambiguous drop"));
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(prepared);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    org.mockito.Mockito.doThrow(unknown)
        .when(viewRepository)
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.COMMIT_STATE_UNKNOWN, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    assertSame(unknown, thrown.getCause());
    verify(viewRepository, times(1))
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(viewOperationAuditEmitter).emitUnknown(prepared, null, ACTING_PRINCIPAL, null, unknown);
  }

  @Test
  public void databaseAuthorizedDropDeletesByNameAfterPreparedViewCapture() {
    existingEnabledDatabase();
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedViewRow());
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(prepared);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    service.deleteView(
        ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    InOrder order =
        inOrder(privilegeMapper, authorizationHandler, viewRepository, viewOperationAuditEmitter);
    order.verify(privilegeMapper).forDelete();
    order.verify(authorizationHandler).checkAccessDecision(any(), any(DatabaseDto.class), any());
    order
        .verify(viewRepository)
        .prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    order
        .verify(viewRepository)
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    order.verify(viewOperationAuditEmitter).emitSuccess(prepared, null, ACTING_PRINCIPAL, null);
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    org.mockito.Mockito.verifyNoMoreInteractions(viewRepository);
  }

  @Test
  public void dropOfAViewThatDisappearsAfterCaptureIsNoSuchViewWithOneAttempt() {
    existingEnabledDatabase();
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedViewRow());
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(prepared);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    org.mockito.Mockito.doThrow(
            new ViewApiException(
                ViewErrorCode.NO_SUCH_VIEW,
                "No such view: "
                    + ViewModelConstants.DATABASE_ID
                    + "."
                    + ViewModelConstants.VIEW_ID))
        .when(viewRepository)
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.NO_SUCH_VIEW, thrown.getErrorCode());
    assertEquals(HttpStatus.NOT_FOUND, thrown.getHttpStatus());
    verify(viewRepository, times(1))
        .deleteById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(viewRepository, times(1))
        .prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
  }

  @Test
  public void databaseProbeOutageOnReadsIsTypedUnavailableWithCauseAndNoAudit() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503"));
    when(databasesService.getAllDatabases()).thenThrow(outage);

    ViewApiException get =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));
    ViewApiException list =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getAllViews(
                    ViewModelConstants.DATABASE_ID, null, 50, "viewId", ACTING_PRINCIPAL));

    for (ViewApiException thrown : new ViewApiException[] {get, list}) {
      assertEquals(ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, thrown.getErrorCode());
      assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
      assertSame(outage, thrown.getCause());
    }
    verifyNoInteractions(viewOperationAuditEmitter, viewRepository);
  }

  @Test
  public void genericPrepareFailuresAreInternalErrorsAuditedAsFailed() {
    existingEnabledDatabase();
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    IllegalStateException writeFault = new IllegalStateException("prepare write fault");
    IllegalStateException deleteFault = new IllegalStateException("prepare delete fault");
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(writeFault);
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(deleteFault);

    ViewApiException write =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));
    ViewApiException delete =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, write.getErrorCode());
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, write.getHttpStatus());
    assertSame(writeFault, write.getCause());
    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, delete.getErrorCode());
    assertSame(deleteFault, delete.getCause());
    verify(viewOperationAuditEmitter, times(2))
        .emitFailed(any(), org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL), any());
    verify(viewOperationAuditEmitter, never()).emitUnknown(any(), any(), any(), any(), any());
  }

  @Test
  public void genericDatabaseProbeFailureOnReadsIsInternalErrorWithCauseAndNoAudit() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    IllegalStateException fault = new IllegalStateException("probe fault");
    when(databasesService.getAllDatabases()).thenThrow(fault);

    ViewApiException get =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));
    ViewApiException list =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getAllViews(
                    ViewModelConstants.DATABASE_ID, null, 50, "viewId", ACTING_PRINCIPAL));

    for (ViewApiException thrown : new ViewApiException[] {get, list}) {
      assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, thrown.getErrorCode());
      assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getHttpStatus());
      assertSame(fault, thrown.getCause());
    }
    verifyNoInteractions(viewOperationAuditEmitter, viewRepository);
  }

  @Test
  public void prepareWriteOutageIsTypedUnavailableAndAuditedAsFailedNotUnknown() {
    existingEnabledDatabase();
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503"));
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(outage);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    assertSame(outage, thrown.getCause());
    verify(viewOperationAuditEmitter, times(1))
        .emitFailed(any(), org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL), any());
    verify(viewOperationAuditEmitter, never()).emitUnknown(any(), any(), any(), any(), any());
    verify(viewOperationAuditEmitter, never()).emitSuccess(any(), any(), any(), any());
    verify(viewRepository, never()).commitCreate(any(), any(), any());
  }

  @Test
  public void prepareDeleteOutageIsTypedUnavailableAndAuditedAsFailedNotUnknown() {
    existingEnabledDatabase();
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503"));
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(outage);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, thrown.getErrorCode());
    assertSame(outage, thrown.getCause());
    verify(viewOperationAuditEmitter, times(1))
        .emitFailed(any(), org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL), any());
    verify(viewOperationAuditEmitter, never()).emitUnknown(any(), any(), any(), any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void disabledAndMissingDatabaseWritesAreAuditedAsFailedWithoutCapture() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(false);
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.putView(
                        ViewModelConstants.createRequestWithoutBaseVersion(),
                        ACTING_PRINCIPAL,
                        true))
            .getErrorCode());
    assertEquals(
        ViewErrorCode.VIEWS_DISABLED,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.deleteView(
                        ViewModelConstants.DATABASE_ID,
                        ViewModelConstants.VIEW_ID,
                        ACTING_PRINCIPAL))
            .getErrorCode());
    verify(viewOperationAuditEmitter, times(2))
        .emitFailed(any(), org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL), any());

    clearInvocations(viewOperationAuditEmitter);
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());
    assertEquals(
        ViewErrorCode.DATABASE_NOT_FOUND,
        assertThrows(
                ViewApiException.class,
                () ->
                    service.putView(
                        ViewModelConstants.createRequestWithoutBaseVersion(),
                        ACTING_PRINCIPAL,
                        true))
            .getErrorCode());
    verify(viewOperationAuditEmitter, times(1))
        .emitFailed(any(), org.mockito.ArgumentMatchers.eq(ACTING_PRINCIPAL), any());
    verify(viewRepository, never()).prepareWrite(any(), any());
    verify(viewRepository, never()).prepareDelete(any(), any());
  }

  @Test
  public void deniedDropDoesNotCaptureOrMutate() {
    existingEnabledDatabase();
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(false);

    assertThrows(
        AccessDeniedException.class,
        () ->
            service.deleteView(
                ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    verify(viewRepository, never()).prepareDelete(any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void absentDropReportsNoSuchViewAfterDatabaseAuthorizationWithoutMutation() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    when(viewRepository.prepareDelete(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.NO_SUCH_VIEW, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @Test
  public void tableCollisionIsRevealedOnlyAfterDatabaseAuthorization() {
    existingEnabledDatabase();
    HouseTable tableOccupant =
        HouseTable.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId(ViewModelConstants.VIEW_ID)
            .entityType("TABLE")
            .build();
    PreparedViewOperation tableCollision = PreparedViewOperation.tableOccupant(tableOccupant);
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(tableCollision);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
    verify(admissionService, never()).admit(any());
  }

  @Test
  public void existingViewPostConflictsAfterAuthorizationAndBeforeAdmission() {
    existingEnabledDatabase();
    PreparedViewOperation existing = PreparedViewOperation.view(capturedViewRow());
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(existing);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.VIEW_ALREADY_EXISTS, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
    verify(admissionService, never()).admit(any());
  }

  @Test
  public void admissionRejectionPreventsCreateAndReplaceCommits() {
    existingEnabledDatabase();
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.observedAbsence())
        .thenReturn(PreparedViewOperation.view(capturedViewRow()));
    ViewApiException admissionFailure =
        new ViewApiException(ViewErrorCode.VIEW_ADMISSION_FAILED, "rejected");
    org.mockito.Mockito.doThrow(admissionFailure).when(admissionService).admit(any());

    assertSame(
        admissionFailure,
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithInitialBaseVersion(),
                    ACTING_PRINCIPAL,
                    false)));
    assertSame(
        admissionFailure,
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false)));
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
  }

  @Test
  public void noOpReplacementReturnsSnapshotPointerWithoutRefreshingPreparedState() {
    existingEnabledDatabase();
    PreparedViewOperation prepared = PreparedViewOperation.view(capturedViewRow());
    ViewCommitOutcome nonFresh =
        ViewCommitOutcome.builder()
            .dto(
                pointerDto()
                    .toBuilder()
                    .metadataLocation(ViewModelConstants.METADATA_LOCATION)
                    .build())
            .committedViewUuid("view-uuid")
            .created(false)
            .build();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(prepared)
        .thenThrow(new AssertionError("service must not refresh after no-op commit"));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewRepository.commitReplace(
            ViewModelConstants.fullyPopulatedRequest(), prepared, ACTING_PRINCIPAL))
        .thenReturn(nonFresh);

    Pair<ViewDto, Boolean> result =
        service.putView(ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false);

    assertEquals(nonFresh.getDto(), result.getFirst());
    assertEquals(Boolean.FALSE, result.getSecond());
    verify(viewRepository, times(1))
        .prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(viewRepository, times(1))
        .commitReplace(ViewModelConstants.fullyPopulatedRequest(), prepared, ACTING_PRINCIPAL);
    verify(viewOperationAuditEmitter)
        .emitSuccess(prepared, nonFresh, ACTING_PRINCIPAL, ViewModelConstants.SOURCE_DIALECT);
    org.mockito.Mockito.verifyNoMoreInteractions(viewRepository);
  }

  @Test
  public void commitStateUnknownMapsTo503PreservesCauseAndEmitsOneUnknownAudit() {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    RuntimeException rawCause = new RuntimeException("raw sql/schema/token must stay internal");
    CommitStateUnknownException unknown = new CommitStateUnknownException(rawCause);
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenThrow(unknown);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.COMMIT_STATE_UNKNOWN, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    assertSame(unknown, thrown.getCause());
    verify(viewRepository, times(1))
        .commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL);
    verify(viewOperationAuditEmitter)
        .emitUnknown(absence, null, ACTING_PRINCIPAL, ViewModelConstants.SOURCE_DIALECT, unknown);
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @ParameterizedTest
  @MethodSource("writeFailureMappings")
  public void createFailureMappingsPreserveCauseStatusAndAvoidOperationSwitch(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(expected, thrown.getErrorCode());
    assertEquals(expectedStatus, thrown.getHttpStatus());
    assertSame(source, thrown.getCause());
    verify(viewRepository, times(1))
        .commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL);
    verify(viewRepository, never()).commitReplace(any(), any(), any());
    verify(viewRepository, never()).deleteById(any(), any());
  }

  @ParameterizedTest
  @MethodSource("readFailureMappings")
  public void getFailureMappingsPreserveCauseAndStatus(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(expected, thrown.getErrorCode());
    assertEquals(expectedStatus, thrown.getHttpStatus());
    assertSame(source, thrown.getCause());
    verifyNoInteractions(authorizationHandler);
  }

  @Test
  public void fatalErrorsPropagateUnchangedFromReadAndWritePaths() {
    existingEnabledDatabase();
    FatalTestError readFatal = new FatalTestError();
    when(viewRepository.findById(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenThrow(readFatal);

    assertSame(
        readFatal,
        assertThrows(
            FatalTestError.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL)));

    PreparedViewOperation absence = PreparedViewOperation.observedAbsence();
    FatalTestError writeFatal = new FatalTestError();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(absence);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(viewRepository.commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL))
        .thenThrow(writeFatal);

    assertSame(
        writeFatal,
        assertThrows(
            FatalTestError.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true)));
    verify(viewRepository, times(1))
        .commitCreate(
            ViewModelConstants.createRequestWithoutBaseVersion(), absence, ACTING_PRINCIPAL);
  }

  /** A JVM-fatal stand-in that no exception translation is allowed to wrap or swallow. */
  private static final class FatalTestError extends Error {
    private FatalTestError() {
      super("fatal");
    }
  }

  @Test
  public void deniedTableCollisionDoesNotRevealOccupantOrAdmit() {
    existingEnabledDatabase();
    HouseTable tableOccupant =
        HouseTable.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId(ViewModelConstants.VIEW_ID)
            .entityType("TABLE")
            .build();
    when(viewRepository.prepareWrite(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(PreparedViewOperation.tableOccupant(tableOccupant));
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(false);

    assertThrows(
        AccessDeniedException.class,
        () ->
            service.putView(
                ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    verify(admissionService, never()).admit(any());
    verify(viewRepository, never()).commitCreate(any(), any(), any());
    verify(viewRepository, never()).commitReplace(any(), any(), any());
  }

  private void existingEnabledDatabase() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
  }

  private static final String COMMITTED_CREATE_UUID = "committed-create-uuid";

  private static ViewCommitOutcome createdOutcome() {
    return ViewCommitOutcome.builder()
        .dto(pointerDto())
        .committedViewUuid(COMMITTED_CREATE_UUID)
        .created(true)
        .build();
  }

  private static ViewDto pointerDto() {
    return ViewDto.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .viewId(ViewModelConstants.VIEW_ID)
        .clusterId(ViewModelConstants.CLUSTER_ID)
        .metadataLocation(ViewModelConstants.METADATA_LOCATION)
        .viewVersion(ViewModelConstants.METADATA_LOCATION)
        .build();
  }

  private static HouseTable capturedViewRow() {
    return HouseTable.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .tableUUID("view-uuid")
        .tableLocation(ViewModelConstants.METADATA_LOCATION)
        .storageType("local")
        .entityType("VIEW")
        .build();
  }

  private void assertDatabaseAuthorization(Privileges privilege) {
    ArgumentCaptor<String> principal = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<DatabaseDto> database = ArgumentCaptor.forClass(DatabaseDto.class);
    ArgumentCaptor<Privileges> checkedPrivilege = ArgumentCaptor.forClass(Privileges.class);
    verify(authorizationHandler)
        .checkAccessDecision(principal.capture(), database.capture(), checkedPrivilege.capture());
    assertEquals(ACTING_PRINCIPAL, principal.getValue());
    assertEquals(ViewModelConstants.DATABASE_ID, database.getValue().getDatabaseId());
    assertEquals(privilege, checkedPrivilege.getValue());
  }

  private static Stream<Arguments> writeFailureMappings() {
    return Stream.of(
        Arguments.of(
            new AlreadyExistsException("View already exists: %s", "existing"),
            ViewErrorCode.VIEW_ALREADY_EXISTS,
            HttpStatus.CONFLICT),
        Arguments.of(
            new BadRequestException("trusted server input"),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR),
        Arguments.of(
            new HouseTableCallerException("caller", new RuntimeException("raw")),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR),
        Arguments.of(
            new IllegalStateException("corrupt"),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR),
        Arguments.of(
            new java.io.UncheckedIOException(new java.io.IOException("io")),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR));
  }

  private static Stream<Arguments> readFailureMappings() {
    return Stream.of(
        Arguments.of(
            new HouseTableNotFoundException("missing", new RuntimeException("404")),
            ViewErrorCode.NO_SUCH_VIEW,
            HttpStatus.NOT_FOUND),
        Arguments.of(
            new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503")),
            ViewErrorCode.VIEW_SERVICE_UNAVAILABLE,
            HttpStatus.SERVICE_UNAVAILABLE),
        Arguments.of(
            new HouseTableCallerException("caller", new RuntimeException("400")),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR),
        Arguments.of(
            new IllegalStateException("corrupt"),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR));
  }
}
