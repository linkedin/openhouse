package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.SystemOnlyLockAccessDeniedException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
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
import org.junit.jupiter.api.function.Executable;
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
        "NONE,NULL,true,true",
        "NONE,SYSTEM,true,true",
        "LEGACY,NULL,true,false",
        "LEGACY,SYSTEM,true,false",
        "SYSTEM_ONLY,NULL,false,false",
        "SYSTEM_ONLY,SyStEm,true,true"
      },
      nullValues = "NULL")
  void readAndOrdinaryWriteMatrix(String reason, String header, boolean canRead, boolean canWrite) {
    lock(reason);
    declaration(header);
    if (canRead) {
      assertSame(current, tables.getTable("db", "table", "owner"));
    } else {
      assertSystemOnlyDenial(
          assertThrows(
              UnsupportedClientOperationException.class,
              () -> tables.getTable("db", "table", "owner")));
    }
    if (canWrite) {
      tables.putTable(request(), "owner", false);
      snapshotWrite(false);
      assertSavedLocks(2);
    } else {
      Class<? extends UnsupportedClientOperationException> expected =
          "SYSTEM_ONLY".equals(reason)
              ? SystemOnlyLockAccessDeniedException.class
              : UnsupportedClientOperationException.class;
      assertThrowsExactly(expected, () -> tables.putTable(request(), "owner", false));
      assertThrowsExactly(expected, () -> snapshotWrite(false));
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
  void systemOnlyReplacementAndRenameRequireDeclaration(String operation, boolean enabled) {
    lock("SYSTEM_ONLY");
    declaration(enabled ? "SYSTEM" : null);
    if (enabled) {
      write(operation);
      if ("rename".equals(operation)) {
        verify(repository).rename(any(), any());
      } else {
        assertSavedLocks(1);
      }
    } else {
      assertSystemOnlyDenial(
          assertThrows(UnsupportedClientOperationException.class, () -> write(operation)));
      verify(repository, never()).save(any());
      verify(repository, never()).rename(any(), any());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"SYSTEM", "USER"})
  void systemOnlyDetailsNeverPrecedeReadOrWriteAuthorization(String actionType) {
    lock("SYSTEM_ONLY");
    declaration(actionType);
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
  void systemOnlySystemActionRequiresDataPermissionsNotLockAdmin() {
    lock("SYSTEM_ONLY");
    declaration("SYSTEM");
    permissions.remove(Privileges.LOCK_ADMIN);
    assertSame(current, tables.getTable("db", "table", "owner"));
    tables.putTable(request(), "owner", false);
    snapshotWrite(false);
    assertSavedLocks(2);
  }

  @Test
  void legacyDeclarationKeepsLockAdminReadsAndPreAuthorizationWriteDenials() {
    lock("LEGACY");
    declaration("SYSTEM");
    permissions.remove(Privileges.LOCK_ADMIN);
    permissions.remove(Privileges.UPDATE_TABLE_METADATA);
    assertThrows(AccessDeniedException.class, () -> tables.getTable("db", "table", "owner"));
    assertLegacyDenial(
        "Table db.table is in locked state and cannot be updated.",
        () -> tables.putTable(request(), "owner", false));
    assertLegacyDenial(
        "Table db.table is in locked state and cannot be written to", () -> snapshotWrite(true));
    assertLegacyDenial(
        "Table db.table is in locked state and cannot be renamed.", () -> write("rename"));
    verify(repository, never()).save(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " SYSTEM", "USER"})
  void invalidDeclarationIsRejectedOnlyWhenSystemOnlyAccessIsEvaluated(String value) {
    lock("SYSTEM_ONLY");
    declaration(value);
    assertThrows(
        RequestValidationFailureException.class, () -> tables.getTable("db", "table", "owner"));
    assertThrows(
        RequestValidationFailureException.class, () -> tables.putTable(request(), "owner", false));
    lock("NONE");
    assertSame(current, tables.getTable("db", "table", "owner"));
  }

  @Test
  void missingRequestContextIsNotSystemAction() {
    lock("SYSTEM_ONLY");
    RequestContextHolder.resetRequestAttributes();
    assertSystemOnlyDenial(
        assertThrows(
            UnsupportedClientOperationException.class,
            () -> tables.getTable("db", "table", "owner")));
  }

  @Test
  void messageDoesNotChooseTheLockReasonAndInactiveLocksDoNotBlock() {
    current =
        current
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(LockState.builder().locked(true).message("SYSTEM_ONLY").build())
                    .build())
            .build();
    declaration("SYSTEM");
    assertThrows(
        UnsupportedClientOperationException.class,
        () -> tables.putTable(request(), "owner", false));
    current =
        current
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder().locked(false).reason(LockReason.SYSTEM_ONLY).build())
                    .build())
            .build();
    declaration(null);
    assertSame(current, tables.getTable("db", "table", "owner"));
    tables.putTable(request(), "owner", false);
    ArgumentCaptor<TableDto> saved = ArgumentCaptor.forClass(TableDto.class);
    verify(repository).save(saved.capture());
    assertNull(saved.getValue().getPolicies());
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
                        .message("maintenance in progress")
                        .creationTime(123)
                        .build())
                .build();
    current = current.toBuilder().policies(policies).build();
  }

  private void declaration(String value) {
    MockHttpServletRequest request = new MockHttpServletRequest();
    if (value != null) {
      request.addHeader(TablesMvcConstants.HTTP_HEADER_ACTION_TYPE, value);
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
      assertEquals(lockState(current), lockState(dto));
    }
  }

  private static LockState lockState(TableDto table) {
    return table.getPolicies() == null ? null : table.getPolicies().getLockState();
  }

  private static void assertLegacyDenial(String message, Executable operation) {
    UnsupportedClientOperationException exception =
        assertThrowsExactly(UnsupportedClientOperationException.class, operation);
    assertEquals(message, exception.getMessage());
  }

  private void assertSystemOnlyDenial(UnsupportedClientOperationException exception) {
    assertInstanceOf(SystemOnlyLockAccessDeniedException.class, exception);
    assertTrue(exception.getMessage().contains("SYSTEM_ONLY"));
    assertTrue(exception.getMessage().contains("db.table"));
    assertTrue(exception.getMessage().contains("maintenance in progress"));
    assertTrue(exception.getMessage().contains("reason-targeted OpenHouse unlock"));
  }
}
