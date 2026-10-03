package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewNameOccupiedException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.audit.ViewOperationAuditEmitter;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.ViewApiException;
import com.linkedin.openhouse.tables.exception.ViewErrorCode;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.model.ViewListResult;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalViewRepository;
import com.linkedin.openhouse.tables.repository.ViewCommitOutcome;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.util.Pair;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

/**
 * Default {@link ViewsService}, modeled after {@code TablesServiceImpl} without reusing it: no
 * snapshots, partition specs, sort orders, or retention semantics. Per-intent order:
 *
 * <pre>
 * GET / LIST: gate -> database existence -> read (no authorization)
 *
 * POST / PUT: gate -> database existence -> prepared capture -> authorization
 *             -> resource type / base-version -> admission -> allocate (create only)
 *             -> commit -> audit
 *
 * DELETE:     gate -> database existence -> authorization -> prepared capture
 *             -> delete -> audit
 * </pre>
 *
 * GET and POST/PUT item responses carry the server-owned {@code clusterId}; every failure is typed
 * with its cause preserved for internal classification, never rendered to the caller.
 */
@Component
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewsServiceImpl implements ViewsService {

  private static final String VIEWS_DISABLED_MESSAGE = "Views are disabled";

  @Autowired private ViewsFeatureGate featureGate;

  @Autowired private DatabasesService databasesService;

  @Autowired private OpenHouseInternalViewRepository viewRepository;

  @Autowired private AuthorizationUtils authorizationUtils;

  @Autowired private ViewAdmissionService admissionService;

  @Autowired private ViewWritePrivilegeMapper privilegeMapper;

  @Autowired private ViewOperationAuditEmitter viewOperationAuditEmitter;

  @Autowired private ClusterProperties clusterProperties;

  private final ViewPaginationAdapter paginationAdapter =
      new ViewPaginationAdapter(this::sourcePage, ViewPaginationAdapter.DEFAULT_SOURCE_PAGE_SIZE);

  @Override
  public ViewDto getView(String databaseId, String viewId, String actingPrincipal) {
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, null);
      return withClusterId(viewRepository.findById(databaseId, viewId));
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
    String sourceDialect = requestBody.getSourceDialect();
    // May stay null if the gate/database check fails before a capture is ever taken; the audit
    // dispatch below null-guards this for exactly that pre-capture case.
    PreparedViewOperation prepared = null;
    ViewCommitOutcome outcome;
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, requestBody.getDefaultNamespace());

      prepared = viewRepository.prepareWrite(databaseId, viewId);
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

      admissionService.admit(requestBody);

      outcome =
          viewAlreadyExists
              ? viewRepository.commitReplace(requestBody, prepared, actingPrincipal)
              : viewRepository.commitCreate(requestBody, prepared, actingPrincipal);
    } catch (RuntimeException e) {
      throw handleWriteFailure(e, prepared, databaseId, viewId, actingPrincipal, sourceDialect);
    }
    // Outside the try: once commit/deleteById returns, SUCCESS is terminal and a subsequent audit-
    // delivery failure must never re-enter the FAILED branch above or retry the already-
    // acknowledged mutation.
    viewOperationAuditEmitter.emitSuccess(prepared, outcome, actingPrincipal, sourceDialect);
    return Pair.of(withClusterId(outcome.getDto()), outcome.isCreated());
  }

  @Override
  public void deleteView(String databaseId, String viewId, String actingPrincipal) {
    PreparedViewOperation prepared = null;
    try {
      requireEnabled(databaseId);
      requireDatabasesExist(databaseId, null);

      Privileges privilege = privilegeMapper.forDelete();
      authorizationUtils.checkDatabasePrivilege(databaseId, actingPrincipal, privilege);

      prepared = viewRepository.prepareDelete(databaseId, viewId);
      if (!prepared.getViewBaseRow().isPresent()) {
        throw new ViewApiException(
            ViewErrorCode.NO_SUCH_VIEW, "No such view: " + databaseId + "." + viewId);
      }
      viewRepository.deleteById(databaseId, viewId);
    } catch (RuntimeException e) {
      throw handleWriteFailure(e, prepared, databaseId, viewId, actingPrincipal, null);
    }
    viewOperationAuditEmitter.emitSuccess(prepared, null, actingPrincipal, null);
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
   * Common write-failure dispatch: an ambiguous commit emits exactly one UNKNOWN audit event and is
   * reclassified as the distinct sanitizable {@link ViewErrorCode#COMMIT_STATE_UNKNOWN}; every
   * other failure emits exactly one FAILED audit event and is rethrown, translated into a {@link
   * ViewApiException} unless it already carries one (including an authorization denial / outage,
   * which keeps its own exact type for the dedicated advice).
   */
  private RuntimeException handleWriteFailure(
      RuntimeException e,
      PreparedViewOperation prepared,
      String databaseId,
      String viewId,
      String actingPrincipal,
      String sourceDialect) {
    // A gate/database/capture failure (or a denied authorization check) leaves prepared null: no
    // row was ever captured, so audit identity falls back to the request's own identifiers.
    PreparedViewOperation preparedForAudit =
        (prepared == null ? PreparedViewOperation.observedAbsence() : prepared)
            .withRequestedIdentity(databaseId, viewId);
    if (e instanceof CommitStateUnknownException) {
      viewOperationAuditEmitter.emitUnknown(
          preparedForAudit, null, actingPrincipal, sourceDialect, e);
      return new ViewApiException(
          ViewErrorCode.COMMIT_STATE_UNKNOWN, "View commit outcome unknown", e);
    }
    viewOperationAuditEmitter.emitFailed(preparedForAudit, actingPrincipal, sourceDialect);
    if (e instanceof ViewApiException
        || e instanceof AccessDeniedException
        || e instanceof AuthorizationServiceException) {
      return e;
    }
    return translateWriteFailure(e);
  }

  private static ViewApiException translateWriteFailure(RuntimeException e) {
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
      // A known precommit transient (e.g. a prepare-time HTS outage), distinct from an
      // unacknowledged publication (CommitStateUnknownException, handled separately above):
      // driven by exception type only, never by matching a message.
      return new ViewApiException(
          ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, "View service unavailable", e);
    }
    // engine BadRequestException (trusted server input), HouseTableCallerException, corrupt-row
    // IllegalStateException, and any other unexpected failure are server faults: caller-input 400s
    // are owned entirely by the API validator before the service is reached.
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
    // HTS identity is case-insensitive (the gate's own probe canonicalizes with Locale.ROOT for
    // the same reason), so a route or defaultNamespace alias that differs only in case from a
    // seeded database id names the same existing database, not a missing one.
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

  private org.springframework.data.domain.Page<ViewDto> sourcePage(
      String databaseId, org.springframework.data.domain.Pageable pageable) {
    return viewRepository.searchViews(databaseId, pageable);
  }
}
