package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec;
import com.linkedin.openhouse.internal.catalog.view.ViewSourceDialectMismatchException;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ReservedViewProperties;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit contract of the single-layer {@link ViewsServiceImpl}: the service captures, reads, lists
 * and deletes through the typed House Table repository, selects/allocates storage for a create
 * only, and commits through the {@link ViewCommitEngine}. Every collaborator is a plain mock with
 * explicit fixtures; {@code @PostConstruct} is not run here (see {@code ViewsServiceStartupTest}).
 */
public class ViewsServiceImplTest {

  private static final String ACTING_PRINCIPAL = "alice";
  private static final String CAPTURED_POINTER = ViewModelConstants.METADATA_LOCATION;
  private static final String COMMITTED_POINTER =
      "file:/warehouse/my_database/my_view/metadata/00013-committed.metadata.json";
  private static final String CAPTURED_VIEW_UUID = "view-uuid";

  /** Deliberately not a UUID, so it can never equal the tentative allocation UUID. */
  private static final String COMMITTED_CREATE_UUID = "committed-create-uuid";

  private ViewsFeatureGate featureGate;
  private DatabasesService databasesService;
  private HouseTableRepository houseTableRepository;
  private ViewCommitEngine viewCommitEngine;
  private FileIOManager fileIOManager;
  private ViewMetadataCodec viewMetadataCodec;
  private InputFile capturedMetadataFile;
  private ViewMetadata storedMetadata;
  private StorageSelector storageSelector;
  private Storage selectedStorage;
  private StorageManager storageManager;
  private AuthorizationUtils authorizationUtils;
  private AuthorizationHandler authorizationHandler;
  private ViewWritePrivilegeMapper privilegeMapper;
  private ClusterProperties clusterProperties;
  private ViewsServiceImpl service;

  @BeforeEach
  public void setup() {
    featureGate = mock(ViewsFeatureGate.class);
    databasesService = mock(DatabasesService.class);
    houseTableRepository = mock(HouseTableRepository.class);
    viewCommitEngine = mock(ViewCommitEngine.class);
    fileIOManager = mock(FileIOManager.class);
    FileIO fileIO = mock(FileIO.class);
    when(fileIOManager.getFileIO(any())).thenReturn(fileIO);
    capturedMetadataFile = mock(InputFile.class);
    when(fileIO.newInputFile(anyString()))
        .thenThrow(new AssertionError("GET reads only the captured pointer"));
    doReturn(capturedMetadataFile).when(fileIO).newInputFile(CAPTURED_POINTER);
    storedMetadata = mock(ViewMetadata.class);
    when(storedMetadata.properties())
        .thenReturn(
            ImmutableMap.of(
                "openhouse.tableCreator",
                ViewModelConstants.VIEW_CREATOR,
                "openhouse.lastModifiedTime",
                String.valueOf(ViewModelConstants.LAST_MODIFIED_TIME)));
    viewMetadataCodec = mock(ViewMetadataCodec.class);
    when(viewMetadataCodec.read(capturedMetadataFile)).thenReturn(storedMetadata);
    storageSelector = mock(StorageSelector.class);
    selectedStorage = mock(Storage.class);
    storageManager = mock(StorageManager.class);
    authorizationHandler = mock(AuthorizationHandler.class);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(true);
    authorizationUtils = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorizationUtils, "authorizationHandler", authorizationHandler);
    privilegeMapper = mock(ViewWritePrivilegeMapper.class);
    clusterProperties = mock(ClusterProperties.class);
    when(clusterProperties.getClusterName()).thenReturn(ViewModelConstants.CLUSTER_ID);
    service = new ViewsServiceImpl();
    ReflectionTestUtils.setField(service, "featureGate", featureGate);
    ReflectionTestUtils.setField(service, "databasesService", databasesService);
    ReflectionTestUtils.setField(service, "authorizationUtils", authorizationUtils);
    ReflectionTestUtils.setField(service, "privilegeMapper", privilegeMapper);
    ReflectionTestUtils.setField(service, "clusterProperties", clusterProperties);
    // The direct collaborators replacing the view repository are injected by type, not name.
    ReflectionTestUtils.setField(service, null, houseTableRepository, HouseTableRepository.class);
    ReflectionTestUtils.setField(service, null, viewCommitEngine, ViewCommitEngine.class);
    ReflectionTestUtils.setField(service, null, storageSelector, StorageSelector.class);
    ReflectionTestUtils.setField(service, null, storageManager, StorageManager.class);
    ReflectionTestUtils.setField(service, null, fileIOManager, FileIOManager.class);
    ReflectionTestUtils.setField(service, null, viewMetadataCodec, ViewMetadataCodec.class);
    ReflectionTestUtils.setField(service, null, new StorageType(), StorageType.class);
  }

  @Test
  public void disabledGateShortCircuitsDatabaseProbesAuthorizationAndViewAccess() {
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
    verifyNoInteractions(
        databasesService,
        houseTableRepository,
        viewCommitEngine,
        storageSelector,
        authorizationHandler);
  }

  @Test
  public void missingDatabaseFailsBeforeCaptureOrAuthorization() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases()).thenReturn(Collections.emptyList());

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.DATABASE_NOT_FOUND, thrown.getErrorCode());
    verifyNoInteractions(houseTableRepository, viewCommitEngine, authorizationHandler);
  }

  @Test
  public void getViewPerformsNoAuthorizationAndPopulatesServerClusterId() {
    existingEnabledDatabase();
    captureForDelete(Optional.of(capturedViewRow()));
    when(clusterProperties.getClusterName()).thenReturn("server-cluster");

    ViewDto result =
        service.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    assertEquals("server-cluster", result.getClusterId());
    verifyNoInteractions(authorizationHandler);
  }

  /**
   * GET maps the pointer from the one typed House Table row and reads creator and modification time
   * from the metadata file it points to: HTS persists neither field.
   */
  @Test
  public void getViewMapsThePointerRowAndReadsCreatorAndModifiedTimeFromItsMetadata() {
    existingEnabledDatabase();
    captureForDelete(Optional.of(capturedViewRow()));

    ViewDto result =
        service.getView(
            ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    assertEquals(
        ViewDto.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .viewId(ViewModelConstants.VIEW_ID)
            .clusterId(ViewModelConstants.CLUSTER_ID)
            .metadataLocation(CAPTURED_POINTER)
            .viewVersion(CAPTURED_POINTER)
            .viewCreator(ViewModelConstants.VIEW_CREATOR)
            .creationTime(ViewModelConstants.CREATION_TIME)
            .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
            .build(),
        result);
    verify(houseTableRepository, times(1)).findViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verify(viewMetadataCodec, times(1)).read(capturedMetadataFile);
    verifyNoMoreInteractions(viewMetadataCodec);
    verifyNoInteractions(viewCommitEngine, storageSelector, storageManager);
  }

  @Test
  public void getViewWithUnreadableOrCreatorlessMetadataIsAnInternalErrorWithCause() {
    existingEnabledDatabase();
    captureForDelete(Optional.of(capturedViewRow()));
    when(storedMetadata.properties()).thenReturn(ImmutableMap.of());

    ViewApiException missingCreator =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));
    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, missingCreator.getErrorCode());
    assertTrue(missingCreator.getCause() instanceof IllegalStateException);

    RuntimeException unreadable = new RuntimeException("metadata file missing");
    when(viewMetadataCodec.read(capturedMetadataFile)).thenThrow(unreadable);
    ViewApiException readFailure =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));
    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, readFailure.getErrorCode());
    assertSame(unreadable, readFailure.getCause());
  }

  @Test
  public void createAuthorizesAtDatabaseLevelBeforeAllocationAndCommit() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    Pair<ViewDto, Boolean> result =
        service.putView(
            ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true);

    assertEquals(
        committedDto(ACTING_PRINCIPAL).toBuilder().clusterId(ViewModelConstants.CLUSTER_ID).build(),
        result.getFirst());
    assertEquals(Boolean.TRUE, result.getSecond());
    InOrder order =
        inOrder(
            featureGate,
            databasesService,
            houseTableRepository,
            privilegeMapper,
            authorizationHandler,
            storageSelector,
            selectedStorage,
            viewCommitEngine);
    order.verify(featureGate).isEnabled(ViewModelConstants.DATABASE_ID);
    order.verify(databasesService).getAllDatabases();
    order.verify(houseTableRepository).findEntityById(viewKey());
    order.verify(privilegeMapper).forCreate();
    order.verify(authorizationHandler).checkAccessDecision(any(), any(DatabaseDto.class), any());
    order
        .verify(storageSelector)
        .selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    order
        .verify(selectedStorage)
        .allocateTableLocation(
            eq(ViewModelConstants.DATABASE_ID),
            eq(ViewModelConstants.VIEW_ID),
            any(),
            eq(ACTING_PRINCIPAL),
            anyMap());
    order.verify(viewCommitEngine).commit(any(ViewCommitIntent.class));
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
  }

  @Test
  public void createReturnsTheMappedDtoAfterOneNeutralCapture() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    Pair<ViewDto, Boolean> result =
        service.putView(
            ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true);

    assertEquals(Boolean.TRUE, result.getSecond());
    assertEquals(ViewModelConstants.DATABASE_ID, result.getFirst().getDatabaseId());
    assertEquals(ViewModelConstants.VIEW_ID, result.getFirst().getViewId());
    assertEquals(COMMITTED_POINTER, result.getFirst().getMetadataLocation());
    verify(houseTableRepository, times(1)).findEntityById(viewKey());
    verify(houseTableRepository, never()).findViewById(any());
  }

  /**
   * Create generates one UUID, allocates the bare root from the per-request selected storage (not
   * the default), and forwards the caller's whole definition.
   */
  @Test
  public void createAllocatesFromTheSelectedStorageAndForwardsUuidRootAndTypeToTheEngine() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    Pair<ViewDto, Boolean> result =
        service.putView(
            ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true);

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    ArgumentCaptor<String> allocatedUuid = ArgumentCaptor.forClass(String.class);
    verify(storageSelector, times(1))
        .selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(selectedStorage, times(1))
        .allocateTableLocation(
            eq(ViewModelConstants.DATABASE_ID),
            eq(ViewModelConstants.VIEW_ID),
            allocatedUuid.capture(),
            eq(ACTING_PRINCIPAL),
            anyMap());
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.TRUE, intent.getValue().getIsCreate());
    assertNull(intent.getValue().getBaseRow(), "An observed absence has no base row.");
    assertEquals("hdfs", intent.getValue().getStorageType());
    assertIntentCarriesRequestedDefinition(intent.getValue());
    UUID.fromString(intent.getValue().getViewUuid());
    assertEquals(allocatedUuid.getValue(), intent.getValue().getViewUuid());
    assertEquals(
        allocatedRoot(intent.getValue().getViewUuid()), intent.getValue().getViewLocation());

    assertEquals(Boolean.TRUE, result.getSecond());
    assertEquals(COMMITTED_POINTER, result.getFirst().getMetadataLocation());
    assertEquals(ViewModelConstants.VIEW_ID, result.getFirst().getViewId());
    verify(houseTableRepository, times(1)).findEntityById(viewKey());
    verify(houseTableRepository, never()).findViewById(any());
    verifyNoInteractions(storageManager);
  }

  private enum WritePath {
    POST_CREATE,
    ABSENT_PUT_CREATE,
    CHANGED_REPLACE,
    NO_OP_REPLACE
  }

  /**
   * The source dialect arrives as the reserved view property, while the engine takes it as the
   * intent's own field and refuses every reserved key among user properties. On every write path
   * the service forwards the property's value (not a representation's), hands the engine the
   * remaining user properties, and leaves the caller's map untouched. Only the exact reserved key
   * is consumed: a case variant is an ordinary user property and must reach the engine.
   */
  @ParameterizedTest
  @EnumSource(WritePath.class)
  public void writesForwardTheReservedSourceDialectToTheIntentAndOnlyUserPropertiesToTheEngine(
      WritePath path) {
    Map<String, String> requested = new LinkedHashMap<>();
    requested.put("owner", "openhouse");
    requested.put(ReservedViewProperties.SOURCE_DIALECT, "trino");
    requested.put("team", "grid");
    requested.put("OpenHouse.view.sourceDialect", "preserve-user-value");
    Map<String, String> requestedBefore = new LinkedHashMap<>(requested);
    Map<String, String> expectedUserProperties = new LinkedHashMap<>();
    expectedUserProperties.put("owner", "openhouse");
    expectedUserProperties.put("team", "grid");
    expectedUserProperties.put("OpenHouse.view.sourceDialect", "preserve-user-value");
    boolean isCreate = path == WritePath.POST_CREATE;
    existingEnabledDatabase();
    CreateUpdateViewRequestBody base;
    switch (path) {
      case POST_CREATE:
        base = ViewModelConstants.createRequestWithoutBaseVersion();
        captureForWrite(Optional.empty());
        when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
        selectableStorage();
        when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());
        break;
      case ABSENT_PUT_CREATE:
        base = ViewModelConstants.createRequestWithInitialBaseVersion();
        captureForWrite(Optional.empty());
        when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
        selectableStorage();
        when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());
        break;
      default:
        base = ViewModelConstants.fullyPopulatedRequest();
        captureForWrite(Optional.of(capturedViewRow()));
        when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
        boolean changed = path == WritePath.CHANGED_REPLACE;
        when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
            .thenReturn(replacedResult(changed ? COMMITTED_POINTER : CAPTURED_POINTER, changed));
        break;
    }
    CreateUpdateViewRequestBody request =
        base.toBuilder()
            .representations(
                Arrays.asList(
                    ViewModelConstants.SPARK_REPRESENTATION,
                    ViewModelConstants.SPARK_REPRESENTATION.toBuilder().dialect("trino").build()))
            .viewProperties(requested)
            .build();

    service.putView(request, ACTING_PRINCIPAL, isCreate);

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals("trino", intent.getValue().getSourceDialect());
    assertEquals(expectedUserProperties, intent.getValue().getViewProperties());
    assertEquals(requestedBefore, requested, "The caller's view properties must not be mutated.");
  }

  /**
   * A body without view properties never passes validation, but the service must still extract the
   * source dialect null-safely rather than fault: no dialect and no user properties reach the
   * engine.
   */
  @Test
  public void createWithoutViewPropertiesForwardsNoSourceDialectAndNoUserProperties() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    service.putView(
        ViewModelConstants.createRequestWithoutBaseVersion()
            .toBuilder()
            .viewProperties(null)
            .build(),
        ACTING_PRINCIPAL,
        true);

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertNull(intent.getValue().getSourceDialect());
    assertTrue(
        intent.getValue().getViewProperties() == null
            || intent.getValue().getViewProperties().isEmpty(),
        "viewProperties: " + intent.getValue().getViewProperties());
  }

  @Test
  public void deniedWriteDoesNotAdmitAllocateOrCommit() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(false);

    assertThrows(
        AccessDeniedException.class,
        () ->
            service.putView(
                ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  @Test
  public void absentPutCreatesOnlyWithInitialVersionAndRejectsStaleTokensBeforeCommit() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    Pair<ViewDto, Boolean> initialResult =
        service.putView(
            ViewModelConstants.createRequestWithInitialBaseVersion(), ACTING_PRINCIPAL, false);

    assertEquals(Boolean.TRUE, initialResult.getSecond());
    clearInvocations(storageSelector, viewCommitEngine);

    ViewApiException stale =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false));
    assertEquals(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, stale.getErrorCode());
    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  @Test
  public void existingPutWithStaleBaseTokenRejectsBeforeCommit() {
    existingEnabledDatabase();
    captureForWrite(
        Optional.of(
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
    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  @Test
  public void replaceCommitFailureMapsToConcurrentModificationWithoutCreateOrDeleteSwitch() {
    existingEnabledDatabase();
    captureForWrite(Optional.of(capturedViewRow()));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    CommitFailedException source = new CommitFailedException("stale replace");
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false));

    assertEquals(ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, thrown.getErrorCode());
    assertSame(source, thrown.getCause());
    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.FALSE, intent.getValue().getIsCreate());
    verify(houseTableRepository, never()).deleteViewById(any());
    verifyNoInteractions(storageSelector);
  }

  /**
   * Replace commits the exact captured row as its CAS token, once, with no House Table refresh and
   * no storage selection, allocation, or new UUID. Its creator is the one the engine kept.
   */
  @Test
  public void replaceCommitsTheExactCapturedRowOnceWithoutRefreshOrAllocation() {
    existingEnabledDatabase();
    HouseTable captured = capturedViewRow();
    when(houseTableRepository.findEntityById(viewKey()))
        .thenReturn(Optional.of(captured))
        .thenThrow(new AssertionError("commit must not refresh the captured row"));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(replacedResult(COMMITTED_POINTER, true));

    Pair<ViewDto, Boolean> result =
        service.putView(ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false);

    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    verify(houseTableRepository, times(1)).findEntityById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    assertSame(captured, intent.getValue().getBaseRow());
    assertEquals(Boolean.FALSE, intent.getValue().getIsCreate());
    assertIntentCarriesRequestedDefinition(intent.getValue());
    assertNull(intent.getValue().getViewUuid(), "Replace never generates a new UUID.");
    assertNull(intent.getValue().getViewLocation(), "Replace never allocates a root.");
    assertNull(intent.getValue().getStorageType(), "Replace never selects storage.");
    assertEquals(
        committedDto(ViewModelConstants.VIEW_CREATOR)
            .toBuilder()
            .clusterId(ViewModelConstants.CLUSTER_ID)
            .build(),
        result.getFirst());
    assertEquals(Boolean.FALSE, result.getSecond());
    verifyNoInteractions(storageSelector, selectedStorage, storageManager);
  }

  /**
   * The managed captured row may be updated in place by the CAS publish. The response creator is
   * the engine's: an HTS row carries no creator, so it must never come from the captured row. There
   * is no second read and no replacement copy.
   */
  @Test
  public void replaceTakesCreatorAndNewPointerFromTheEngineWithoutRefreshingTheManagedRow() {
    existingEnabledDatabase();
    AtomicReference<String> location = new AtomicReference<>(CAPTURED_POINTER);
    HouseTable managed = mock(HouseTable.class);
    when(managed.getEntityType()).thenReturn("VIEW");
    when(managed.getDatabaseId()).thenReturn(ViewModelConstants.DATABASE_ID);
    when(managed.getTableId()).thenReturn(ViewModelConstants.VIEW_ID);
    when(managed.getTableUUID()).thenReturn(CAPTURED_VIEW_UUID);
    when(managed.getStorageType()).thenReturn("local");
    when(managed.getTableLocation()).thenAnswer(invocation -> location.get());
    when(managed.getTableCreator()).thenReturn(null);
    captureForWrite(Optional.of(managed));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenAnswer(
            invocation -> {
              assertSame(managed, invocation.<ViewCommitIntent>getArgument(0).getBaseRow());
              location.set(COMMITTED_POINTER);
              return replacedResult(COMMITTED_POINTER, true)
                  .toBuilder()
                  .viewCreator("engine-kept-creator")
                  .build();
            });

    Pair<ViewDto, Boolean> result =
        service.putView(ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false);

    assertEquals("engine-kept-creator", result.getFirst().getViewCreator());
    assertEquals(COMMITTED_POINTER, result.getFirst().getMetadataLocation());
    verify(houseTableRepository, times(1)).findEntityById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
  }

  @Test
  public void missingDefaultNamespaceDatabaseFailsBeforeCapture() {
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
    verifyNoInteractions(houseTableRepository, viewCommitEngine);
  }

  @Test
  public void existingDefaultNamespaceDatabaseAllowsCreateToProceed() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Arrays.asList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build(),
                DatabaseDto.builder().databaseId("dependency_database").build()));
    CreateUpdateViewRequestBody request =
        ViewModelConstants.createRequestWithInitialBaseVersion()
            .toBuilder()
            .defaultNamespace(Arrays.asList("dependency_database"))
            .build();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forPut(false)).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenReturn(createdResult());

    Pair<ViewDto, Boolean> result = service.putView(request, ACTING_PRINCIPAL, false);

    assertEquals(Boolean.TRUE, result.getSecond());
    assertEquals(COMMITTED_POINTER, result.getFirst().getMetadataLocation());
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
  }

  /**
   * GET and DELETE both use the typed view lookup, so an absent key and a same-name table read
   * alike as NO_SUCH_VIEW, without write-authorization leakage on the read.
   */
  @Test
  public void absentOrWrongTypeReadAndDropReturnNoSuchViewWithoutWriteAuthorizationLeakage() {
    existingEnabledDatabase();
    captureForDelete(Optional.empty());
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
    verify(houseTableRepository, times(2)).findViewById(viewKey());
    verify(houseTableRepository, never()).findEntityById(any());
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  /**
   * Only the one delete attempt is wrapped: an ambiguous typed delete may have landed, so it is an
   * unknown mutation carrying the House Table cause.
   */
  @Test
  public void ambiguousDropIsOneAttemptUnknownCarryingTheHtsCause() {
    existingEnabledDatabase();
    HouseTable row = capturedViewRow();
    captureForDelete(Optional.of(row));
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    HouseTableRepositoryStateUnknownException ambiguous =
        new HouseTableRepositoryStateUnknownException("", new RuntimeException("504"));
    when(houseTableRepository.deleteViewById(viewKey())).thenThrow(ambiguous);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.COMMIT_STATE_UNKNOWN, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    CommitStateUnknownException unknown =
        assertInstanceOf(CommitStateUnknownException.class, thrown.getCause());
    assertSame(ambiguous, unknown.getCause());
    // One typed capture and one typed delete only: no refresh, generic delete, or purge.
    verify(houseTableRepository, times(1)).findViewById(viewKey());
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine, storageSelector);
  }

  /**
   * DELETE is one typed capture after authorization and one typed name-based hard delete: no
   * reread, generic/table delete overload, purge, engine, or storage work.
   */
  @Test
  public void databaseAuthorizedDropDeletesByNameAfterTypedViewCapture() {
    existingEnabledDatabase();
    HouseTable row = capturedViewRow();
    captureForDelete(Optional.of(row));
    when(houseTableRepository.deleteViewById(viewKey())).thenReturn(true);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    service.deleteView(
        ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL);

    InOrder order = inOrder(privilegeMapper, authorizationHandler, houseTableRepository);
    order.verify(privilegeMapper).forDelete();
    order.verify(authorizationHandler).checkAccessDecision(any(), any(DatabaseDto.class), any());
    order.verify(houseTableRepository).findViewById(viewKey());
    order.verify(houseTableRepository).deleteViewById(viewKey());
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine, storageSelector, storageManager);
  }

  /**
   * The typed delete returns false when the name became absent, or now holds a table, after the
   * capture: the ordinary NO_SUCH_VIEW outcome after one attempt, with no refresh or retry.
   */
  @Test
  public void dropOfAViewThatDisappearsAfterCaptureIsNoSuchViewWithOneAttempt() {
    existingEnabledDatabase();
    captureForDelete(Optional.of(capturedViewRow()));
    when(houseTableRepository.deleteViewById(viewKey())).thenReturn(false);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.NO_SUCH_VIEW, thrown.getErrorCode());
    assertEquals(HttpStatus.NOT_FOUND, thrown.getHttpStatus());
    verify(houseTableRepository, times(1)).findViewById(viewKey());
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine);
  }

  /**
   * Only the state-unknown subtype is ambiguous. A sibling House Table failure (the common
   * supertype, so a catch widened to it is caught) is a known failure keeping its raw cause.
   */
  @Test
  public void otherDeleteFailuresAreInternalErrorsWithTheRawCauseAfterOneAttempt() {
    existingEnabledDatabase();
    captureForDelete(Optional.of(capturedViewRow()));
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    HouseTableRepositoryException failure =
        new HouseTableRepositoryException("rejected", new RuntimeException("400"));
    when(houseTableRepository.deleteViewById(viewKey())).thenThrow(failure);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, thrown.getErrorCode());
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getHttpStatus());
    assertSame(failure, thrown.getCause());
    // One typed capture and one typed delete only: no refresh, generic delete, or purge.
    verify(houseTableRepository, times(1)).findViewById(viewKey());
    verify(houseTableRepository, times(1)).deleteViewById(viewKey());
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine);
  }

  @Test
  public void databaseProbeOutageOnReadsIsTypedUnavailableWithCause() {
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
    verifyNoInteractions(houseTableRepository);
  }

  @Test
  public void genericCaptureFailuresAreInternalErrors() {
    existingEnabledDatabase();
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    IllegalStateException writeFault = new IllegalStateException("capture write fault");
    IllegalStateException deleteFault = new IllegalStateException("capture delete fault");
    when(houseTableRepository.findEntityById(viewKey())).thenThrow(writeFault);
    when(houseTableRepository.findViewById(viewKey())).thenThrow(deleteFault);

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
    verifyNoInteractions(viewCommitEngine);
  }

  @Test
  public void genericDatabaseProbeFailureOnReadsIsInternalErrorWithCause() {
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
    verifyNoInteractions(houseTableRepository);
  }

  @Test
  public void writeCaptureOutageIsTypedUnavailableNotUnknown() {
    existingEnabledDatabase();
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503"));
    when(houseTableRepository.findEntityById(viewKey())).thenThrow(outage);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    assertSame(outage, thrown.getCause());
    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  /**
   * The delete-attempt wrapper must not reach the capture: a precommit House Table outage stays the
   * raw cause of a known unavailability, never an unknown mutation.
   */
  @Test
  public void deleteCaptureOutageIsTypedUnavailableAndNeverWrappedAsUnknown() {
    existingEnabledDatabase();
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    HouseTableRepositoryStateUnknownException outage =
        new HouseTableRepositoryStateUnknownException("down", new RuntimeException("503"));
    when(houseTableRepository.findViewById(viewKey())).thenThrow(outage);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, thrown.getErrorCode());
    assertSame(outage, thrown.getCause());
    verify(houseTableRepository, never()).deleteViewById(any());
    verifyNoInteractions(viewCommitEngine);
  }

  @Test
  public void disabledAndMissingDatabaseWritesFailWithoutCapture() {
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
    verifyNoInteractions(houseTableRepository, viewCommitEngine);
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

    verifyNoInteractions(houseTableRepository, viewCommitEngine);
  }

  @Test
  public void absentDropReportsNoSuchViewAfterDatabaseAuthorizationWithoutMutation() {
    existingEnabledDatabase();
    captureForDelete(Optional.empty());
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.deleteView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL));

    assertEquals(ViewErrorCode.NO_SUCH_VIEW, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.DELETE_TABLE);
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  /**
   * The neutral write capture keeps a TABLE occupant (the same reference) without exposing it as a
   * view base; the collision is revealed only after database authorization, before allocation.
   */
  @Test
  public void tableCollisionIsRevealedOnlyAfterDatabaseAuthorization() {
    existingEnabledDatabase();
    HouseTable tableOccupant =
        HouseTable.builder()
            .databaseId(ViewModelConstants.DATABASE_ID)
            .tableId(ViewModelConstants.VIEW_ID)
            .tableLocation(CAPTURED_POINTER)
            .storageType("local")
            .entityType("TABLE")
            .build();
    captureForWrite(Optional.of(tableOccupant));
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  @Test
  public void existingViewPostConflictsAfterAuthorizationAndBeforeCommit() {
    existingEnabledDatabase();
    captureForWrite(Optional.of(capturedViewRow()));
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.VIEW_ALREADY_EXISTS, thrown.getErrorCode());
    assertDatabaseAuthorization(Privileges.CREATE_TABLE);
    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  /** An allocation failure is a known internal error, not an unknown commit. */
  @Test
  public void createAllocationFailureIsInternalErrorWithoutCommit() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(storageSelector.selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID))
        .thenReturn(selectedStorage);
    when(selectedStorage.getType()).thenReturn(StorageType.HDFS);
    IllegalStateException allocationFault = new IllegalStateException("allocation fault");
    when(selectedStorage.allocateTableLocation(any(), any(), any(), any(), anyMap()))
        .thenThrow(allocationFault);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.INTERNAL_VIEW_ERROR, thrown.getErrorCode());
    assertSame(allocationFault, thrown.getCause());
    verifyNoInteractions(viewCommitEngine);
  }

  @Test
  public void noOpReplacementReturnsSnapshotPointerWithoutRefreshingCapturedState() {
    existingEnabledDatabase();
    HouseTable captured = capturedViewRow();
    when(houseTableRepository.findEntityById(viewKey()))
        .thenReturn(Optional.of(captured))
        .thenThrow(new AssertionError("service must not refresh after no-op commit"));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class)))
        .thenReturn(replacedResult(CAPTURED_POINTER, false));

    Pair<ViewDto, Boolean> result =
        service.putView(ViewModelConstants.fullyPopulatedRequest(), ACTING_PRINCIPAL, false);

    assertEquals(CAPTURED_POINTER, result.getFirst().getMetadataLocation());
    assertEquals(CAPTURED_POINTER, result.getFirst().getViewVersion());
    assertEquals(Boolean.FALSE, result.getSecond());
    verify(houseTableRepository, times(1)).findEntityById(viewKey());
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(storageSelector);
  }

  @Test
  public void commitStateUnknownMapsTo503PreservesCauseAfterOneAttempt() {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    RuntimeException rawCause = new RuntimeException("raw sql/schema/token must stay internal");
    CommitStateUnknownException unknown = new CommitStateUnknownException(rawCause);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenThrow(unknown);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(ViewErrorCode.COMMIT_STATE_UNKNOWN, thrown.getErrorCode());
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, thrown.getHttpStatus());
    assertSame(unknown, thrown.getCause());
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  @ParameterizedTest
  @MethodSource("writeFailureMappings")
  public void createFailureMappingsPreserveCauseStatusAndAvoidOperationSwitch(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    assertEquals(expected, thrown.getErrorCode());
    assertEquals(expectedStatus, thrown.getHttpStatus());
    assertSame(source, thrown.getCause());
    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.TRUE, intent.getValue().getIsCreate());
    verify(storageSelector, times(1))
        .selectStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
    verify(houseTableRepository, never()).deleteViewById(any());
  }

  @ParameterizedTest
  @MethodSource("replaceFailureMappings")
  public void replaceFailureMappingsPreserveCauseWithoutRetry(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    HouseTable captured = capturedViewRow();
    CreateUpdateViewRequestBody request =
        ViewModelConstants.fullyPopulatedRequest()
            .toBuilder()
            .baseMetadataLocation(CAPTURED_POINTER)
            .build();
    captureForWrite(Optional.of(captured));
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class, () -> service.putView(request, ACTING_PRINCIPAL, false));

    assertEquals(expected, thrown.getErrorCode());
    assertEquals(expectedStatus, thrown.getHttpStatus());
    assertSame(source, thrown.getCause());
    ArgumentCaptor<ViewCommitIntent> intent = ArgumentCaptor.forClass(ViewCommitIntent.class);
    verify(viewCommitEngine, times(1)).commit(intent.capture());
    assertEquals(Boolean.FALSE, intent.getValue().getIsCreate());
    assertSame(captured, intent.getValue().getBaseRow());
    verify(houseTableRepository, never()).deleteViewById(any());
    verifyNoInteractions(storageSelector);
  }

  @ParameterizedTest
  @MethodSource("readFailureMappings")
  public void getFailureMappingsPreserveCauseAndStatus(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    when(houseTableRepository.findViewById(viewKey())).thenThrow(source);

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
    when(houseTableRepository.findViewById(viewKey())).thenThrow(readFatal);

    assertSame(
        readFatal,
        assertThrows(
            FatalTestError.class,
            () ->
                service.getView(
                    ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID, ACTING_PRINCIPAL)));

    FatalTestError writeFatal = new FatalTestError();
    captureForWrite(Optional.empty());
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    selectableStorage();
    when(viewCommitEngine.commit(any(ViewCommitIntent.class))).thenThrow(writeFatal);

    assertSame(
        writeFatal,
        assertThrows(
            FatalTestError.class,
            () ->
                service.putView(
                    ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true)));
    verify(viewCommitEngine, times(1)).commit(any(ViewCommitIntent.class));
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
    captureForWrite(Optional.of(tableOccupant));
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(authorizationHandler.checkAccessDecision(any(), any(DatabaseDto.class), any()))
        .thenReturn(false);

    assertThrows(
        AccessDeniedException.class,
        () ->
            service.putView(
                ViewModelConstants.createRequestWithoutBaseVersion(), ACTING_PRINCIPAL, true));

    verifyNoInteractions(storageSelector, viewCommitEngine);
  }

  /**
   * Each typed House Table page is reframed by the requested pageable, never the source page's own
   * pageable: framed by the request this stale-total page is the last one, while the source page's
   * own pageable would claim more pages. Entries stay identifier-only, in source order.
   */
  @Test
  public void listFramesTypedHtsPagesByTheRequestedPageableAndMapsIdentifiersOnlyInSourceOrder() {
    existingEnabledDatabase();
    Pageable requested =
        PageRequest.of(2, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE, Sort.by("tableId"));
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested))
        .thenReturn(
            new PageImpl<>(
                Arrays.asList(listedViewRow("view_b"), listedViewRow("view_a")),
                PageRequest.of(0, 4, Sort.by("databaseId")),
                60));
    String continuation =
        new ViewPageTokenCodec()
            .encode(
                new ViewPageCursor(
                    ViewModelConstants.DATABASE_ID,
                    "viewId",
                    ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE,
                    2,
                    0));

    ViewListResult result =
        service.getAllViews(
            ViewModelConstants.DATABASE_ID, continuation, 50, "viewId", ACTING_PRINCIPAL);

    assertEquals(
        Arrays.asList(
            ViewModelConstants.sparseListDto("view_b"), ViewModelConstants.sparseListDto("view_a")),
        result.getResults(),
        "List entries stay identifier-only in source order; pointer and creator fields are unset.");
    assertNull(result.getNextPageToken());
    verify(houseTableRepository, times(1))
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested);
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine, storageSelector, storageManager, authorizationHandler);
  }

  @Test
  public void listDefaultPageSizeUsesOneHtsFetchForOneHundredViews() {
    existingEnabledDatabase();
    Pageable requested = PageRequest.of(0, 100, Sort.by("tableId"));
    List<HouseTable> rows =
        IntStream.range(0, 100)
            .mapToObj(index -> listedViewRow("view_" + index))
            .collect(Collectors.toList());
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested))
        .thenReturn(new PageImpl<>(rows, requested, 200));

    ViewListResult result =
        service.getAllViews(ViewModelConstants.DATABASE_ID, null, 100, "viewId", ACTING_PRINCIPAL);

    assertEquals(100, result.getResults().size());
    ViewPageCursor continuation = new ViewPageTokenCodec().decode(result.getNextPageToken());
    assertEquals(100, continuation.getPageSize());
    assertEquals(1, continuation.getSourcePageIndex());
    assertEquals(0, continuation.getOffset());
    verify(houseTableRepository, times(1))
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, requested);
    verifyNoMoreInteractions(houseTableRepository);
  }

  /** Short and empty nonterminal pages keep their reported totals; only the terminal page ends. */
  @Test
  public void listKeepsShortAndEmptyNonterminalPagesDistinctFromTheTerminalPage() {
    existingEnabledDatabase();
    Pageable shortNonterminal =
        PageRequest.of(0, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE, Sort.by("tableId"));
    Pageable emptyNonterminal =
        PageRequest.of(1, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE, Sort.by("tableId"));
    Pageable terminal =
        PageRequest.of(2, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE, Sort.by("tableId"));
    when(houseTableRepository.findAllViewsByDatabaseId(
            ViewModelConstants.DATABASE_ID, shortNonterminal))
        .thenReturn(
            new PageImpl<>(
                Collections.singletonList(listedViewRow(ViewModelConstants.VIEW_ID)),
                shortNonterminal,
                3L * ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE));
    when(houseTableRepository.findAllViewsByDatabaseId(
            ViewModelConstants.DATABASE_ID, emptyNonterminal))
        .thenReturn(
            new PageImpl<>(
                Collections.<HouseTable>emptyList(),
                emptyNonterminal,
                3L * ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE));
    when(houseTableRepository.findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, terminal))
        .thenReturn(
            new PageImpl<>(
                Collections.<HouseTable>emptyList(),
                terminal,
                2L * ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE + 1));

    ViewListResult result =
        service.getAllViews(ViewModelConstants.DATABASE_ID, null, 50, "viewId", ACTING_PRINCIPAL);

    assertEquals(
        Collections.singletonList(ViewModelConstants.sparseListDto(ViewModelConstants.VIEW_ID)),
        result.getResults());
    assertNull(result.getNextPageToken());
    InOrder order = inOrder(houseTableRepository);
    order
        .verify(houseTableRepository)
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, shortNonterminal);
    order
        .verify(houseTableRepository)
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, emptyNonterminal);
    order
        .verify(houseTableRepository)
        .findAllViewsByDatabaseId(ViewModelConstants.DATABASE_ID, terminal);
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine);
  }

  /** A typed-list contract violation or outage is never an empty page: one read, cause kept. */
  @ParameterizedTest
  @MethodSource("listFailureMappings")
  public void listTypedHtsFailuresKeepTheirClassificationAndCauseAfterOneRead(
      RuntimeException source, ViewErrorCode expected, HttpStatus expectedStatus) {
    existingEnabledDatabase();
    when(houseTableRepository.findAllViewsByDatabaseId(
            eq(ViewModelConstants.DATABASE_ID), any(Pageable.class)))
        .thenThrow(source);

    ViewApiException thrown =
        assertThrows(
            ViewApiException.class,
            () ->
                service.getAllViews(
                    ViewModelConstants.DATABASE_ID, null, 2, "viewId", ACTING_PRINCIPAL));

    assertEquals(expected, thrown.getErrorCode());
    assertEquals(expectedStatus, thrown.getHttpStatus());
    assertSame(source, thrown.getCause());
    verify(houseTableRepository, times(1))
        .findAllViewsByDatabaseId(eq(ViewModelConstants.DATABASE_ID), any(Pageable.class));
    verifyNoMoreInteractions(houseTableRepository);
    verifyNoInteractions(viewCommitEngine);
  }

  /**
   * House Table identity is case-insensitive, so request ids may differ in case from the stored row
   * and the engine's pointer. Acknowledged create/replace responses are named by the requested ids;
   * a failed replace and a drop at the same requested key still complete as typed outcomes.
   */
  @Test
  public void acknowledgedWriteResponsesUseRequestedIdsForACaseDifferentStoredRow() {
    String requestedDatabaseId = "My_Database";
    String requestedViewId = "My_View";
    HouseTablePrimaryKey requestedKey = primaryKey(requestedDatabaseId, requestedViewId);
    HouseTable stored = capturedViewRow();
    when(featureGate.isEnabled(requestedDatabaseId)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
    when(houseTableRepository.findEntityById(requestedKey))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(stored));
    when(houseTableRepository.findViewById(requestedKey)).thenReturn(Optional.of(stored));
    when(houseTableRepository.deleteViewById(requestedKey)).thenReturn(true);
    when(privilegeMapper.forCreate()).thenReturn(Privileges.CREATE_TABLE);
    when(privilegeMapper.forPut(true)).thenReturn(Privileges.UPDATE_TABLE_METADATA);
    when(privilegeMapper.forDelete()).thenReturn(Privileges.DELETE_TABLE);
    selectableStorage(requestedDatabaseId, requestedViewId);
    doReturn(createdResult())
        .doReturn(replacedResult(COMMITTED_POINTER, true))
        .doThrow(new CommitFailedException("stale replace"))
        .when(viewCommitEngine)
        .commit(any(ViewCommitIntent.class));
    CreateUpdateViewRequestBody create =
        ViewModelConstants.createRequestWithoutBaseVersion()
            .toBuilder()
            .databaseId(requestedDatabaseId)
            .viewId(requestedViewId)
            .build();
    CreateUpdateViewRequestBody replace =
        ViewModelConstants.fullyPopulatedRequest()
            .toBuilder()
            .databaseId(requestedDatabaseId)
            .viewId(requestedViewId)
            .build();

    Pair<ViewDto, Boolean> created = service.putView(create, ACTING_PRINCIPAL, true);
    Pair<ViewDto, Boolean> replaced = service.putView(replace, ACTING_PRINCIPAL, false);
    assertThrows(ViewApiException.class, () -> service.putView(replace, ACTING_PRINCIPAL, false));
    service.deleteView(requestedDatabaseId, requestedViewId, ACTING_PRINCIPAL);

    for (ViewDto response : Arrays.asList(created.getFirst(), replaced.getFirst())) {
      assertEquals(requestedDatabaseId, response.getDatabaseId());
      assertEquals(requestedViewId, response.getViewId());
    }
    verify(viewCommitEngine, times(3)).commit(any(ViewCommitIntent.class));
    verify(houseTableRepository, times(1)).deleteViewById(requestedKey);
  }

  private void existingEnabledDatabase() {
    when(featureGate.isEnabled(ViewModelConstants.DATABASE_ID)).thenReturn(true);
    when(databasesService.getAllDatabases())
        .thenReturn(
            Collections.singletonList(
                DatabaseDto.builder().databaseId(ViewModelConstants.DATABASE_ID).build()));
  }

  /** The neutral POST/PUT capture at the requested key. */
  private void captureForWrite(Optional<HouseTable> occupant) {
    when(houseTableRepository.findEntityById(viewKey())).thenReturn(occupant);
  }

  /** The typed GET/DELETE lookup at the requested key. */
  private void captureForDelete(Optional<HouseTable> viewRow) {
    when(houseTableRepository.findViewById(viewKey())).thenReturn(viewRow);
  }

  private void selectableStorage() {
    selectableStorage(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
  }

  private void selectableStorage(String databaseId, String viewId) {
    when(storageSelector.selectStorage(databaseId, viewId)).thenReturn(selectedStorage);
    when(selectedStorage.getType()).thenReturn(StorageType.HDFS);
    when(selectedStorage.allocateTableLocation(
            eq(databaseId), eq(viewId), any(), eq(ACTING_PRINCIPAL), anyMap()))
        .thenAnswer(invocation -> allocatedRoot(invocation.getArgument(2)));
  }

  private static String allocatedRoot(String viewUuid) {
    return "hdfs:/warehouse/"
        + ViewModelConstants.DATABASE_ID
        + "/"
        + ViewModelConstants.VIEW_ID
        + "-"
        + viewUuid;
  }

  private static HouseTablePrimaryKey viewKey() {
    return primaryKey(ViewModelConstants.DATABASE_ID, ViewModelConstants.VIEW_ID);
  }

  private static HouseTablePrimaryKey primaryKey(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  /** Engine acknowledgement of a create, with a UUID distinct from any tentative UUID. */
  private static ViewCommitResult createdResult() {
    return ViewCommitResult.builder()
        .pointer(
            ViewPointer.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .viewId(ViewModelConstants.VIEW_ID)
                .metadataLocation(COMMITTED_POINTER)
                .storageType("hdfs")
                .creationTime(ViewModelConstants.CREATION_TIME)
                .build())
        .viewUuid(COMMITTED_CREATE_UUID)
        .viewCreator(ACTING_PRINCIPAL)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .created(true)
        .metadataChanged(true)
        .build();
  }

  private static ViewCommitResult replacedResult(String pointerLocation, boolean metadataChanged) {
    return ViewCommitResult.builder()
        .pointer(
            ViewPointer.builder()
                .databaseId(ViewModelConstants.DATABASE_ID)
                .viewId(ViewModelConstants.VIEW_ID)
                .metadataLocation(pointerLocation)
                .storageType("local")
                .creationTime(ViewModelConstants.CREATION_TIME)
                .build())
        .viewUuid(CAPTURED_VIEW_UUID)
        .viewCreator(ViewModelConstants.VIEW_CREATOR)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .created(false)
        .metadataChanged(metadataChanged)
        .build();
  }

  /** The published committed-DTO mapping, before the server cluster id is applied. */
  private static ViewDto committedDto(String creator) {
    return ViewDto.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .viewId(ViewModelConstants.VIEW_ID)
        .metadataLocation(COMMITTED_POINTER)
        .viewVersion(COMMITTED_POINTER)
        .viewCreator(creator)
        .creationTime(ViewModelConstants.CREATION_TIME)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .build();
  }

  /**
   * Mirrors a real HTS row: no creator or modification time, which live only in view metadata. The
   * stored metadata's creator differs from the acting principal to catch creator substitution.
   */
  private static HouseTable capturedViewRow() {
    return HouseTable.builder()
        .databaseId(ViewModelConstants.DATABASE_ID)
        .tableId(ViewModelConstants.VIEW_ID)
        .tableUUID(CAPTURED_VIEW_UUID)
        .tableLocation(CAPTURED_POINTER)
        .creationTime(ViewModelConstants.CREATION_TIME)
        .storageType("local")
        .entityType("VIEW")
        .build();
  }

  /** A fully populated row, so the list DTO's unset pointer/creator fields are meaningful. */
  private static HouseTable listedViewRow(String viewId) {
    return capturedViewRow()
        .toBuilder()
        .tableId(viewId)
        .tableCreator(ViewModelConstants.VIEW_CREATOR)
        .lastModifiedTime(ViewModelConstants.LAST_MODIFIED_TIME)
        .build();
  }

  /**
   * The caller's definition is forwarded to the engine. The reserved source-dialect property
   * becomes the intent's source dialect and the engine receives only the remaining user properties;
   * everything else is unchanged. Expected values are the request fixture's own constants, and the
   * schema is parsed by Iceberg directly.
   */
  private static void assertIntentCarriesRequestedDefinition(ViewCommitIntent intent) {
    assertEquals(ViewModelConstants.DATABASE_ID, intent.getDatabaseId());
    assertEquals(ViewModelConstants.VIEW_ID, intent.getViewId());
    assertEquals(
        Collections.singletonList(
            SqlViewRepresentationIntent.builder()
                .sql(ViewModelConstants.VIEW_SQL)
                .dialect(ViewModelConstants.SOURCE_DIALECT)
                .build()),
        intent.getRepresentations());
    assertEquals(ViewModelConstants.SOURCE_DIALECT, intent.getSourceDialect());
    assertTrue(
        SchemaParser.fromJson(ViewModelConstants.VIEW_SCHEMA_LITERAL)
            .sameSchema(intent.getSchema()),
        "schema: " + intent.getSchema());
    assertEquals(ViewModelConstants.DEFAULT_CATALOG, intent.getDefaultCatalog());
    assertEquals(Namespace.of(ViewModelConstants.DATABASE_ID), intent.getDefaultNamespace());
    Map<String, String> userProperties = new LinkedHashMap<>(ViewModelConstants.VIEW_PROPERTIES);
    userProperties.remove(ReservedViewProperties.SOURCE_DIALECT);
    assertEquals(userProperties, intent.getViewProperties());
    assertEquals(ACTING_PRINCIPAL, intent.getCreator());
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

  private static Stream<Arguments> replaceFailureMappings() {
    return Stream.concat(
        Stream.of(
            Arguments.of(
                new ViewSourceDialectMismatchException("private_database", "private_view"),
                ViewErrorCode.INVALID_VIEW_DEFINITION,
                HttpStatus.BAD_REQUEST)),
        writeFailureMappings());
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

  private static Stream<Arguments> listFailureMappings() {
    return Stream.of(
        Arguments.of(
            new IllegalStateException("House Table listed a non-view row for the view route"),
            ViewErrorCode.INTERNAL_VIEW_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR),
        Arguments.of(
            new HouseTableRepositoryStateUnknownException("", new RuntimeException("504")),
            ViewErrorCode.VIEW_SERVICE_UNAVAILABLE,
            HttpStatus.SERVICE_UNAVAILABLE));
  }
}
