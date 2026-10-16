package com.linkedin.openhouse.tables.services;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.ENTITY_TYPE_VIEW;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec;
import com.linkedin.openhouse.internal.catalog.view.ViewNameOccupiedException;
import com.linkedin.openhouse.internal.catalog.view.ViewSourceDialectMismatchException;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.util.Pair;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

/**
 * Default {@link ViewsService}, modeled after {@code TablesServiceImpl} without reusing it: no
 * snapshots, partition specs, sort orders, or retention semantics. Per-intent order:
 *
 * <pre>
 * GET:  gate -> database existence -> HTS read -> metadata read (no authorization)
 * LIST: gate -> database existence -> HTS read (no authorization)
 *
 * POST / PUT: gate -> database existence -> prepared capture -> authorization
 *             -> resource type / base-version -> allocate (create only)
 *             -> commit
 *
 * DELETE:     gate -> database existence -> authorization -> prepared capture
 *             -> delete
 * </pre>
 *
 * Captures, reads, lists and deletes use the typed {@link HouseTableRepository}; a create selects
 * and allocates storage here, above the commit-only {@link ViewCommitEngine}. GET and POST/PUT
 * responses carry the server-owned {@code clusterId}; failures are typed with their cause kept for
 * internal classification and never rendered to the caller.
 */
@Component
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewsServiceImpl implements ViewsService {

  private static final String VIEWS_DISABLED_MESSAGE = "Views are disabled";

  @Autowired private ViewsFeatureGate featureGate;

  @Autowired private DatabasesService databasesService;

  @Autowired private HouseTableRepository houseTableRepository;

  @Autowired private ViewCommitEngine viewCommitEngine;

  @Autowired private StorageSelector storageSelector;

  @Autowired private FileIOManager fileIOManager;

  @Autowired private ViewMetadataCodec viewMetadataCodec;

  @Autowired private StorageType storageType;

  @Autowired private StorageManager storageManager;

  @Autowired private AuthorizationUtils authorizationUtils;

  @Autowired private ViewWritePrivilegeMapper privilegeMapper;

  @Autowired private ClusterProperties clusterProperties;

  private final ViewPaginationAdapter paginationAdapter =
      new ViewPaginationAdapter(this::sourcePage, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE);

  /**
   * Resolves the cluster's default storage eagerly so an unresolvable default fails startup. The
   * result is deliberately not retained: create always selects storage per request.
   */
  @PostConstruct
  void validateDefaultStorage() {
    storageManager.getDefaultStorage();
  }

  @Override
  public ViewDto getView(String databaseId, String viewId, String actingPrincipal) {
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, null);
      HouseTable viewRow =
          houseTableRepository
              .findViewById(primaryKey(databaseId, viewId))
              .orElseThrow(() -> noSuchView(databaseId, viewId));
      requireCompletePersistedPointer(viewRow);
      return withClusterId(toPointerDto(viewRow));
    } catch (ViewApiException e) {
      throw e;
    } catch (RuntimeException e) {
      throw translateReadFailure(e);
    }
  }

  @Override
  public ViewListResult getAllViews(
      String databaseId, String pageToken, int size, String sortBy, String actingPrincipal) {
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, null);
      return paginationAdapter.list(databaseId, pageToken, size, sortBy);
    } catch (ViewApiException e) {
      throw e;
    } catch (RuntimeException e) {
      throw translateReadFailure(e);
    }
  }

  @Override
  public Pair<ViewDto, Boolean> putView(
      CreateUpdateViewRequestBody requestBody, String actingPrincipal, boolean failOnExist) {
    String databaseId = requestBody.getDatabaseId();
    String viewId = requestBody.getViewId();
    ViewCommitResult result;
    ViewDto dto;
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, requestBody.getDefaultNamespace());

      Optional<HouseTable> occupant =
          houseTableRepository.findEntityById(primaryKey(databaseId, viewId));
      PreparedViewOperation prepared =
          occupant
              .map(
                  row ->
                      ENTITY_TYPE_VIEW.equals(row.getEntityType())
                          ? PreparedViewOperation.view(row)
                          : PreparedViewOperation.tableOccupant(row))
              .orElseGet(PreparedViewOperation::observedAbsence);
      boolean viewAlreadyExists = prepared.getViewBaseRow().isPresent();
      Privileges privilege =
          failOnExist ? privilegeMapper.forCreate() : privilegeMapper.forPut(viewAlreadyExists);
      authorizationUtils.checkDatabasePrivilege(databaseId, actingPrincipal, privilege);

      boolean occupantIsTable = prepared.getOccupantRow().isPresent() && !viewAlreadyExists;
      if (occupantIsTable) {
        throw new ViewApiException(
            ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE,
            "Name already exists as a table: " + databaseId + "." + viewId);
      }
      if (failOnExist && viewAlreadyExists) {
        throw new ViewApiException(
            ViewErrorCode.VIEW_ALREADY_EXISTS, "View already exists: " + databaseId + "." + viewId);
      }

      if (!failOnExist) {
        checkBaseVersion(
            databaseId, viewId, requestBody.getBaseMetadataLocation(), prepared, viewAlreadyExists);
      }

      result = commitView(requestBody, prepared, actingPrincipal, !viewAlreadyExists);
      dto = toCommittedDto(databaseId, viewId, result);
    } catch (RuntimeException e) {
      throw handleWriteFailure(e);
    }
    return Pair.of(withClusterId(dto), result.isCreated());
  }

  @Override
  public void deleteView(String databaseId, String viewId, String actingPrincipal) {
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, null);

      Privileges privilege = privilegeMapper.forDelete();
      authorizationUtils.checkDatabasePrivilege(databaseId, actingPrincipal, privilege);

      PreparedViewOperation prepared =
          houseTableRepository
              .findViewById(primaryKey(databaseId, viewId))
              .map(PreparedViewOperation::view)
              .orElseGet(PreparedViewOperation::observedAbsence);
      if (!prepared.getViewBaseRow().isPresent()) {
        throw noSuchView(databaseId, viewId);
      }
      deleteCapturedView(databaseId, viewId);
    } catch (RuntimeException e) {
      throw handleWriteFailure(e);
    }
  }

  private void deleteCapturedView(String databaseId, String viewId) {
    boolean dropped;
    try {
      dropped = houseTableRepository.deleteViewById(primaryKey(databaseId, viewId));
    } catch (HouseTableRepositoryStateUnknownException e) {
      throw new CommitStateUnknownException(e);
    }
    if (!dropped) {
      // The view was dropped or replaced by a non-view after capture: no retry, same as absent.
      throw noSuchView(databaseId, viewId);
    }
  }

  private ViewCommitResult commitView(
      CreateUpdateViewRequestBody requestBody,
      PreparedViewOperation prepared,
      String actingPrincipal,
      boolean isCreate) {
    String viewUuid = null;
    String viewLocation = null;
    String storageType = null;
    if (isCreate) {
      viewUuid = UUID.randomUUID().toString();
      Storage storage =
          storageSelector.selectStorage(requestBody.getDatabaseId(), requestBody.getViewId());
      storageType = storage.getType().getValue();
      // The bare allocated root, passed verbatim: the engine stores it as ViewMetadata.location()
      // and lays metadata files out flat directly under it, per its frozen contract.
      viewLocation =
          storage.allocateTableLocation(
              requestBody.getDatabaseId(),
              requestBody.getViewId(),
              viewUuid,
              actingPrincipal,
              requestBody.getViewProperties() == null
                  ? Collections.emptyMap()
                  : requestBody.getViewProperties());
    }
    Schema schema = IcebergSchemaHelper.getSchemaFromSchemaJson(requestBody.getSchema());
    return viewCommitEngine.commit(
        ViewCommitIntent.builder()
            .databaseId(requestBody.getDatabaseId())
            .viewId(requestBody.getViewId())
            .schema(schema)
            .representations(toEngineRepresentations(requestBody.getRepresentations()))
            .sourceDialect(requestBody.getSourceDialect())
            .defaultCatalog(requestBody.getDefaultCatalog())
            .defaultNamespace(toNamespace(requestBody.getDefaultNamespace()))
            .viewProperties(requestBody.getViewProperties())
            .creator(actingPrincipal)
            .isCreate(isCreate)
            .baseRow(prepared.getViewBaseRow().orElse(null))
            .viewUuid(viewUuid)
            .viewLocation(viewLocation)
            .storageType(storageType)
            .build());
  }

  private void checkBaseVersion(
      String databaseId,
      String viewId,
      String baseToken,
      PreparedViewOperation prepared,
      boolean viewAlreadyExists) {
    if (!viewAlreadyExists) {
      if (!ValidatorConstants.INITIAL_TABLE_VERSION.equals(baseToken)) {
        throw new ViewApiException(
            ViewErrorCode.CONCURRENT_VIEW_MODIFICATION,
            "Stale base version for absent view: " + databaseId + "." + viewId);
      }
      return;
    }
    String currentPointer = prepared.getViewBaseRow().get().getTableLocation();
    if (!currentPointer.equals(baseToken)) {
      throw new ViewApiException(
          ViewErrorCode.CONCURRENT_VIEW_MODIFICATION,
          "Concurrent view modification: " + databaseId + "." + viewId);
    }
  }

  /**
   * Write-failure dispatch: an ambiguous commit becomes {@link ViewErrorCode#COMMIT_STATE_UNKNOWN};
   * any other failure is rethrown as a {@link ViewApiException}, except authorization failures,
   * which keep their own type for the dedicated advice.
   */
  private RuntimeException handleWriteFailure(RuntimeException e) {
    if (e instanceof CommitStateUnknownException) {
      return new ViewApiException(
          ViewErrorCode.COMMIT_STATE_UNKNOWN, "View commit outcome unknown", e);
    }
    if (e instanceof ViewApiException
        || e instanceof AccessDeniedException
        || e instanceof AuthorizationServiceException) {
      return e;
    }
    return translateWriteFailure(e);
  }

  private static ViewApiException translateWriteFailure(RuntimeException e) {
    if (e instanceof ViewSourceDialectMismatchException) {
      return new ViewApiException(
          ViewErrorCode.INVALID_VIEW_DEFINITION, "sourceDialect must match the current view", e);
    }
    if (e instanceof AlreadyExistsException) {
      return new ViewApiException(ViewErrorCode.VIEW_ALREADY_EXISTS, "View already exists", e);
    }
    if (e instanceof ViewNameOccupiedException) {
      return new ViewApiException(
          ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE, "Name already exists as a table", e);
    }
    if (e instanceof CommitFailedException) {
      return new ViewApiException(
          ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, "Concurrent view modification", e);
    }
    if (e instanceof HouseTableRepositoryStateUnknownException) {
      // A known precommit transient (e.g. a capture-time HTS outage), distinct from an
      // unacknowledged publication (CommitStateUnknownException, handled separately above):
      // driven by exception type only, never by matching a message.
      return new ViewApiException(
          ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, "View service unavailable", e);
    }
    // Other engine BadRequestException (trusted server input), HouseTableCallerException,
    // corrupt-row IllegalStateException, and unexpected failures remain server faults.
    return new ViewApiException(
        ViewErrorCode.INTERNAL_VIEW_ERROR, "Unexpected view service failure", e);
  }

  private static ViewApiException translateReadFailure(RuntimeException e) {
    if (e instanceof HouseTableNotFoundException) {
      return new ViewApiException(ViewErrorCode.NO_SUCH_VIEW, "No such view", e);
    }
    if (e instanceof HouseTableRepositoryStateUnknownException) {
      return new ViewApiException(
          ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, "View service unavailable", e);
    }
    // HouseTableCallerException (server-side 4xx to a trusted call), corrupt-row
    // IllegalStateException, and any other unexpected read failure are server faults.
    return new ViewApiException(
        ViewErrorCode.INTERNAL_VIEW_ERROR, "Unexpected view service failure", e);
  }

  private void requireEnabled(String databaseId) {
    if (!featureGate.isEnabled(databaseId)) {
      throw new ViewApiException(ViewErrorCode.VIEWS_DISABLED, VIEWS_DISABLED_MESSAGE);
    }
  }

  private void requireDatabasesExist(String databaseId, List<String> defaultNamespace) {
    // HTS identity is case-insensitive, so compare database ids lowercased.
    Set<String> existingDatabaseIds =
        databasesService.getAllDatabases().stream()
            .map(DatabaseDto::getDatabaseId)
            .map(id -> id.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    if (!existingDatabaseIds.contains(databaseId.toLowerCase(Locale.ROOT))) {
      throw new ViewApiException(
          ViewErrorCode.DATABASE_NOT_FOUND, "Database not found: " + databaseId);
    }
    if (defaultNamespace != null) {
      for (String namespaceDatabaseId : defaultNamespace) {
        if (!existingDatabaseIds.contains(namespaceDatabaseId.toLowerCase(Locale.ROOT))) {
          throw new ViewApiException(
              ViewErrorCode.DATABASE_NOT_FOUND, "Database not found: " + namespaceDatabaseId);
        }
      }
    }
  }

  private ViewDto withClusterId(ViewDto dto) {
    return dto.toBuilder().clusterId(clusterProperties.getClusterName()).build();
  }

  private Page<ViewDto> sourcePage(String databaseId, Pageable pageable) {
    Page<HouseTable> rows = houseTableRepository.findAllViewsByDatabaseId(databaseId, pageable);
    List<ViewDto> dtos =
        rows.getContent().stream()
            .map(
                row ->
                    ViewDto.builder()
                        .viewId(row.getTableId())
                        .databaseId(row.getDatabaseId())
                        .build())
            .collect(Collectors.toList());
    return new PageImpl<>(dtos, pageable, rows.getTotalElements());
  }

  private static List<SqlViewRepresentationIntent> toEngineRepresentations(
      List<ViewRepresentation> representations) {
    if (representations == null) {
      return Collections.emptyList();
    }
    return representations.stream()
        .map(
            representation ->
                SqlViewRepresentationIntent.builder()
                    .sql(representation.getSql())
                    .dialect(representation.getDialect())
                    .build())
        .collect(Collectors.toList());
  }

  private static Namespace toNamespace(List<String> defaultNamespace) {
    if (defaultNamespace == null || defaultNamespace.isEmpty()) {
      return Namespace.empty();
    }
    return Namespace.of(defaultNamespace.toArray(new String[0]));
  }

  private static ViewDto toCommittedDto(String databaseId, String viewId, ViewCommitResult result) {
    ViewPointer pointer = result.getPointer();
    return ViewDto.builder()
        .databaseId(databaseId)
        .viewId(viewId)
        .metadataLocation(pointer.getMetadataLocation())
        .viewVersion(pointer.getMetadataLocation())
        .viewCreator(result.getViewCreator())
        .creationTime(pointer.getCreationTime())
        .lastModifiedTime(result.getLastModifiedTime())
        .build();
  }

  /** HTS stores only the pointer; creator and modification time live in the view metadata. */
  private ViewDto toPointerDto(HouseTable row) {
    FileIO fileIO = fileIOManager.getFileIO(storageType.fromString(row.getStorageType()));
    Map<String, String> properties =
        viewMetadataCodec.read(fileIO.newInputFile(row.getTableLocation())).properties();
    String creator = properties.get(getCanonicalFieldName("tableCreator"));
    if (StringUtils.isBlank(creator)) {
      throw new IllegalStateException(
          "Corrupt view metadata for " + row.getDatabaseId() + "." + row.getTableId());
    }
    String lastModifiedTime = properties.get(getCanonicalFieldName("lastModifiedTime"));
    return ViewDto.builder()
        .viewId(row.getTableId())
        .databaseId(row.getDatabaseId())
        .metadataLocation(row.getTableLocation())
        .viewVersion(row.getTableLocation())
        .viewCreator(creator)
        .creationTime(row.getCreationTime())
        .lastModifiedTime(lastModifiedTime == null ? 0L : Long.parseLong(lastModifiedTime))
        .build();
  }

  /**
   * A valid persisted VIEW row always carries both facts; requiring both is this completeness
   * invariant, not an optional new field. Corrupt persisted metadata is a sanitized server fault,
   * never reinterpreted as absence. Checked on the HTS row before any metadata is read.
   */
  private static void requireCompletePersistedPointer(HouseTable row) {
    if (StringUtils.isBlank(row.getTableLocation()) || StringUtils.isBlank(row.getStorageType())) {
      throw new IllegalStateException(
          "Corrupt view pointer for " + row.getDatabaseId() + "." + row.getTableId());
    }
  }

  private static HouseTablePrimaryKey primaryKey(String databaseId, String viewId) {
    return HouseTablePrimaryKey.builder().databaseId(databaseId).tableId(viewId).build();
  }

  private static ViewApiException noSuchView(String databaseId, String viewId) {
    return new ViewApiException(
        ViewErrorCode.NO_SUCH_VIEW, "No such view: " + databaseId + "." + viewId);
  }
}
