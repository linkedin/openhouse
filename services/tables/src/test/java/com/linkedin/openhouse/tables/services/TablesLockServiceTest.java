package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.security.access.AccessDeniedException;

class TablesLockServiceTest {
  private TablesServiceImpl service;
  private TableDto table;

  @BeforeEach
  void setUp() {
    service = new TablesServiceImpl();
    service.openHouseInternalRepository = mock(OpenHouseInternalRepository.class);
    service.authorizationUtils = mock(AuthorizationUtils.class);
    table =
        TableDto.builder()
            .databaseId("db")
            .tableId("table")
            .tableUUID("generation")
            .tableUri("db.table")
            .tableLocation("metadata.json")
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    when(service.openHouseInternalRepository.findById(any())).thenAnswer(i -> Optional.of(table));
    when(service.openHouseInternalRepository.save(any())).thenAnswer(i -> i.getArgument(0));
  }

  @Test
  void matchingCleanupRetryDoesNotWriteOrRefreshLock() {
    table =
        table.toBuilder().policies(Policies.builder().lockState(cleanupLock("owner")).build()).build();
    service.createLock("db", "table", cleanupRequest(), "owner");
    verify(service.openHouseInternalRepository, never()).save(any());
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "owner", Privileges.LOCK_ADMIN);
  }

  @Test
  void cleanupRetryFromAnotherOwnerCannotOverwriteLock() {
    table =
        table.toBuilder().policies(Policies.builder().lockState(cleanupLock("other")).build()).build();
    assertThrows(
        AlreadyExistsException.class,
        () -> service.createLock("db", "table", cleanupRequest(), "owner"));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupCreationRequiresSystemAdminInAdditionToLegacyPrivilege() {
    doThrow(new AccessDeniedException("not an admin"))
        .when(service.authorizationUtils)
        .checkTablePrivilege(table, "owner", Privileges.SYSTEM_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () -> service.createLock("db", "table", cleanupRequest(), "owner"));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyCreationDoesNotRequireSystemAdmin() {
    service.createLock(
        "db", "table", CreateUpdateLockRequestBody.builder().locked(true).build(), "owner");
    verify(service.authorizationUtils, never())
        .checkTablePrivilege(any(), any(), eq(Privileges.SYSTEM_ADMIN));
    verify(service.openHouseInternalRepository).save(any());
  }

  @Test
  void cleanupUnlockRequiresSystemAdmin() {
    table =
        table.toBuilder().policies(Policies.builder().lockState(cleanupLock("owner")).build()).build();
    doThrow(new AccessDeniedException("not an admin"))
        .when(service.authorizationUtils)
        .checkTablePrivilege(table, "owner", Privileges.SYSTEM_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () ->
            service.deleteLock(
                "db", "table", "owner", LockReason.TIER3_AUTO_CLEANUP, "generation", "owner"));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupRetryStillRequiresLegacyLockPrivilege() {
    table =
        table.toBuilder().policies(Policies.builder().lockState(cleanupLock("owner")).build()).build();
    doThrow(new AccessDeniedException("not a lock admin"))
        .when(service.authorizationUtils)
        .checkLockTablePrivilege(table, "owner", Privileges.LOCK_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () -> service.createLock("db", "table", cleanupRequest(), "owner"));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void genericTableWriteCannotIntroduceCleanupLock(boolean stageReplace) {
    CreateUpdateTableRequestBody request =
        CreateUpdateTableRequestBody.builder()
            .databaseId("db")
            .tableId("table")
            .stageReplace(stageReplace)
            .policies(Policies.builder().lockState(cleanupLock("owner")).build())
            .build();
    assertThrows(
        RequestValidationFailureException.class, () -> service.putTable(request, "owner", false));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void snapshotWriteCannotIntroduceCleanupLock() throws Exception {
    IcebergSnapshotsServiceImpl snapshotsService = new IcebergSnapshotsServiceImpl();
    snapshotsService.openHouseInternalRepository = service.openHouseInternalRepository;
    snapshotsService.tablesMapper = mock(TablesMapper.class);
    snapshotsService.authorizationUtils = service.authorizationUtils;
    snapshotsService.readBridgeStripProtection = mock(ReadBridgeStripProtection.class);
    when(
            snapshotsService.tablesMapper.toTableDto(
                any(TableDto.class), any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(table);
    when(snapshotsService.readBridgeStripProtection.prepare(any(), any())).thenReturn(table);
    IcebergSnapshotsRequestBody request =
        IcebergSnapshotsRequestBody.builder()
            .createUpdateTableRequestBody(
                CreateUpdateTableRequestBody.builder()
                    .clusterId("cluster")
                    .policies(Policies.builder().lockState(cleanupLock("owner")).build())
                    .build())
            .build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> snapshotsService.putIcebergSnapshots("db", "table", request, "owner"));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  private CreateUpdateLockRequestBody cleanupRequest() {
    return CreateUpdateLockRequestBody.builder()
        .locked(true)
        .reason(LockReason.TIER3_AUTO_CLEANUP)
        .expectedTableUUID("generation")
        .build();
  }

  private LockState cleanupLock(String owner) {
    return LockState.builder()
        .locked(true)
        .reason(LockReason.TIER3_AUTO_CLEANUP)
        .tableUUID("generation")
        .lockOwner(owner)
        .creationTime(1)
        .build();
  }
}
