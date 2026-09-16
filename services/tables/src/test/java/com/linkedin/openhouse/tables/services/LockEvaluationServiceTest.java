package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class LockEvaluationServiceTest {
  private OpenHouseInternalRepository repository;
  private TablesServiceImpl tables;
  private IcebergSnapshotsServiceImpl snapshots;
  private TableDto current;
  private EnumSet<Privileges> permissions;

  @BeforeEach
  void setUp() throws Exception {
    RequestContextHolder.resetRequestAttributes();
    permissions = EnumSet.allOf(Privileges.class);
    permissions.remove(Privileges.SYSTEM_ADMIN);
    AuthorizationHandler handler = mock(AuthorizationHandler.class);
    when(handler.checkAccessDecision(anyString(), any(TableDto.class), any()))
        .thenAnswer(invocation -> permissions.contains(invocation.getArgument(2)));
    when(handler.checkAccessDecision(anyString(), any(DatabaseDto.class), any()))
        .thenAnswer(invocation -> permissions.contains(invocation.getArgument(2)));
    AuthorizationUtils authorization = new AuthorizationUtils();
    ReflectionTestUtils.setField(authorization, "authorizationHandler", handler);
    TablesMapper mapper = Mappers.getMapper(TablesMapper.class);
    ReflectionTestUtils.setField(mapper, "policiesSpecMapper", new PoliciesSpecMapper());
    ReadBridgeStripProtection protection = mock(ReadBridgeStripProtection.class);
    when(protection.prepare(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    repository = mock(OpenHouseInternalRepository.class);
    current =
        TableDto.builder()
            .databaseId("db")
            .tableId("table")
            .tableUri("cluster.db.table")
            .clusterId("cluster")
            .tableUUID("uuid")
            .tableCreator("owner")
            .tableLocation("v1")
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    when(repository.findById(any()))
        .thenAnswer(
            invocation ->
                "table".equals(((TableDtoPrimaryKey) invocation.getArgument(0)).getTableId())
                    ? Optional.of(current)
                    : Optional.empty());
    when(repository.findTableRefById(any())).thenAnswer(invocation -> Optional.of(current));
    when(repository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
    tables = new TablesServiceImpl();
    tables.openHouseInternalRepository = repository;
    tables.authorizationUtils = authorization;
    tables.authorizationHandler = handler;
    tables.tablesMapper = mapper;
    tables.readBridgeStripProtection = protection;
    snapshots = new IcebergSnapshotsServiceImpl();
    snapshots.openHouseInternalRepository = repository;
    snapshots.authorizationUtils = authorization;
    snapshots.tablesMapper = mapper;
    snapshots.readBridgeStripProtection = protection;
  }

  @AfterEach
  void clearRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "NONE,NULL,true,true", "NONE,true,true,true",
        "LEGACY,NULL,true,false", "LEGACY,true,true,false",
        "TIER3_AUTO_CLEANUP,NULL,false,false", "TIER3_AUTO_CLEANUP,false,false,false",
        "TIER3_AUTO_CLEANUP,TrUe,true,true"
      },
      nullValues = "NULL")
  void readAndOrdinaryWriteMatrix(String reason, String header, boolean canRead, boolean canWrite) {
    lock(reason);
    declaration(header);
    if (canRead) {
      assertSame(current, tables.getTable("db", "table", "owner"));
    } else {
      assertCleanupDenial(
          assertThrows(
              UnsupportedClientOperationException.class,
              () -> tables.getTable("db", "table", "owner")));
    }
    if (canWrite) {
      tables.putTable(request(), "owner", false);
      snapshotWrite(false);
      assertSavedLocks(2);
    } else {
      assertThrows(
          UnsupportedClientOperationException.class,
          () -> tables.putTable(request(), "owner", false));
      assertThrows(UnsupportedClientOperationException.class, () -> snapshotWrite(false));
      verify(repository, never()).save(any());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "tableReplace,false",
    "tableReplace,true",
    "snapshotReplace,false",
    "snapshotReplace,true",
    "rename,false",
    "rename,true"
  })
  void cleanupReplacementAndRenameRequireDeclaration(String operation, boolean enabled) {
    lock("TIER3_AUTO_CLEANUP");
    declaration(Boolean.toString(enabled));
    if (enabled) {
      write(operation);
      if ("rename".equals(operation)) {
        verify(repository).rename(any(), any());
      } else {
        assertSavedLocks(1);
      }
    } else {
      assertCleanupDenial(
          assertThrows(UnsupportedClientOperationException.class, () -> write(operation)));
      verify(repository, never()).save(any());
      verify(repository, never()).rename(any(), any());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanupDetailsNeverPrecedeReadOrWriteAuthorization(boolean enabled) {
    lock("TIER3_AUTO_CLEANUP");
    declaration(Boolean.toString(enabled));
    permissions.remove(Privileges.GET_TABLE_METADATA);
    permissions.remove(Privileges.UPDATE_TABLE_METADATA);
    assertThrows(AccessDeniedException.class, () -> tables.getTable("db", "table", "owner"));
    assertThrows(AccessDeniedException.class, () -> tables.putTable(request(), "owner", false));
    assertThrows(AccessDeniedException.class, () -> write("tableReplace"));
    assertThrows(AccessDeniedException.class, () -> snapshotWrite(true));
    assertThrows(AccessDeniedException.class, () -> write("rename"));
    verify(repository, never()).save(any());
    verify(repository, never()).rename(any(), any());
  }

  @Test
  void cleanupSystemActionRequiresDataPermissionsNotLockAdmin() {
    lock("TIER3_AUTO_CLEANUP");
    declaration("true");
    permissions.remove(Privileges.LOCK_ADMIN);
    assertSame(current, tables.getTable("db", "table", "owner"));
    tables.putTable(request(), "owner", false);
    snapshotWrite(false);
    assertSavedLocks(2);
  }

  @Test
  void legacyDeclarationCannotBypassLockAdminOrWriteDenial() {
    lock("LEGACY");
    declaration("true");
    permissions.remove(Privileges.LOCK_ADMIN);
    assertThrows(AccessDeniedException.class, () -> tables.getTable("db", "table", "owner"));
    assertThrows(UnsupportedClientOperationException.class, () -> write("tableReplace"));
    assertThrows(UnsupportedClientOperationException.class, () -> snapshotWrite(true));
    assertThrows(UnsupportedClientOperationException.class, () -> write("rename"));
    verify(repository, never()).save(any());
  }

  @Test
  void invalidDeclarationIsRejectedOnlyWhenCleanupAccessIsEvaluated() {
    lock("TIER3_AUTO_CLEANUP");
    declaration("yes");
    assertThrows(
        RequestValidationFailureException.class, () -> tables.getTable("db", "table", "owner"));
    assertThrows(
        RequestValidationFailureException.class, () -> tables.putTable(request(), "owner", false));
    assertNotNull(tables.getLock("db", "table", "owner").getLockState());
    lock("NONE");
    assertSame(current, tables.getTable("db", "table", "owner"));
  }

  @Test
  void messageDoesNotChooseTheLockReasonAndInactiveLocksDoNotBlock() {
    current =
        current
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder().locked(true).message("TIER3_AUTO_CLEANUP").build())
                    .build())
            .build();
    declaration("true");
    assertThrows(
        UnsupportedClientOperationException.class,
        () -> tables.putTable(request(), "owner", false));
    current =
        current
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder()
                            .locked(false)
                            .reason(LockReason.TIER3_AUTO_CLEANUP)
                            .build())
                    .build())
            .build();
    declaration(null);
    assertSame(current, tables.getTable("db", "table", "owner"));
    tables.putTable(request(), "owner", false);
    assertSavedLocks(1);
  }

  @Test
  void statusGrantUnlockAndDropKeepTheirOwnControls() {
    lock("TIER3_AUTO_CLEANUP");
    declaration(null);
    assertNotNull(tables.getLock("db", "table", "owner").getLockState());
    declaration("true");
    assertThrows(
        UnsupportedClientOperationException.class,
        () ->
            tables.updateAclPolicies(
                "db",
                "table",
                UpdateAclPoliciesRequestBody.builder()
                    .operation(UpdateAclPoliciesRequestBody.Operation.GRANT)
                    .role("role")
                    .principal("grantee")
                    .build(),
                "owner"));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> tables.deleteLock("db", "table", "owner"));
    assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            tables.deleteLock(
                "db", "table", LockReason.TIER3_AUTO_CLEANUP, "uuid", "wrong-owner", "owner"));
    permissions.remove(Privileges.LOCK_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () ->
            tables.deleteLock(
                "db", "table", LockReason.TIER3_AUTO_CLEANUP, "uuid", "owner", "owner"));
    permissions.remove(Privileges.DELETE_TABLE);
    assertThrows(AccessDeniedException.class, () -> tables.deleteTable("db", "table", "owner"));
    verify(repository, never()).save(any());
    verify(repository, never()).deleteById(any());
  }

  private void lock(String reason) {
    Policies policies =
        "NONE".equals(reason)
            ? null
            : Policies.builder()
                .sharingEnabled(true)
                .lockState(
                    LockState.builder()
                        .locked(true)
                        .reason(LockReason.valueOf(reason))
                        .message("eligible for cleanup")
                        .lockOwner("owner")
                        .tableUUID("uuid")
                        .creationTime(123)
                        .build())
                .build();
    current = current.toBuilder().policies(policies).build();
  }

  private void declaration(String value) {
    MockHttpServletRequest request = new MockHttpServletRequest();
    if (value != null) {
      request.addHeader(TablesMvcConstants.HTTP_HEADER_SYSTEM_ACTION, value);
    }
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  private CreateUpdateTableRequestBody request() {
    return CreateUpdateTableRequestBody.builder()
        .databaseId("db")
        .tableId("table")
        .clusterId("cluster")
        .baseTableVersion("v1")
        .tableProperties(Collections.singletonMap("updated", "value"))
        .build();
  }

  private void snapshotWrite(boolean replace) {
    snapshots.putIcebergSnapshots(
        "db",
        "table",
        IcebergSnapshotsRequestBody.builder()
            .baseTableVersion("v1")
            .createUpdateTableRequestBody(request().toBuilder().replaceCommit(replace).build())
            .build(),
        "owner");
  }

  private void write(String operation) {
    switch (operation) {
      case "tableReplace":
        tables.putTable(request().toBuilder().stageReplace(true).build(), "owner", true);
        break;
      case "snapshotReplace":
        snapshotWrite(true);
        break;
      case "rename":
        tables.renameTable("db", "table", "db", "renamed", "owner");
        break;
      default:
        throw new AssertionError(operation);
    }
  }

  private void assertSavedLocks(int count) {
    ArgumentCaptor<TableDto> saved = ArgumentCaptor.forClass(TableDto.class);
    verify(repository, times(count)).save(saved.capture());
    for (TableDto dto : saved.getAllValues()) {
      assertEquals(current.getPolicies(), dto.getPolicies());
    }
  }

  private void assertCleanupDenial(UnsupportedClientOperationException exception) {
    assertTrue(exception.getMessage().contains("TIER3_AUTO_CLEANUP"));
    assertTrue(exception.getMessage().contains("db.table"));
    assertTrue(exception.getMessage().contains("eligible for cleanup"));
    assertTrue(exception.getMessage().contains("Tier 2"));
    assertTrue(exception.getMessage().contains("unlock"));
  }
}
