package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
    when(service.openHouseInternalRepository.findById(any()))
        .thenAnswer(invocation -> Optional.of(table));
    when(service.openHouseInternalRepository.save(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void legacyLockStoresLegacyReasonWithoutOwnerOrGeneration() {
    service.createLock(
        "db", "table", CreateUpdateLockRequestBody.builder().locked(true).build(), "owner");

    ArgumentCaptor<TableDto> savedTable = ArgumentCaptor.forClass(TableDto.class);
    verify(service.openHouseInternalRepository).save(savedTable.capture());
    assertEquals(LockReason.LEGACY, savedTable.getValue().getPolicies().getLockState().getReason());
    assertNull(savedTable.getValue().getPolicies().getLockState().getLockOwner());
    assertNull(savedTable.getValue().getPolicies().getLockState().getTableUUID());
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "owner", Privileges.LOCK_ADMIN);
  }

  @Test
  void legacyLockReplacesExistingLegacyLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder()
                            .locked(true)
                            .reason(LockReason.LEGACY)
                            .message("Earlier legacy lock")
                            .build())
                    .build())
            .build();

    service.createLock(
        "db",
        "table",
        CreateUpdateLockRequestBody.builder().locked(true).message("Later legacy lock").build(),
        "another-owner");

    ArgumentCaptor<TableDto> savedTable = ArgumentCaptor.forClass(TableDto.class);
    verify(service.openHouseInternalRepository).save(savedTable.capture());
    assertEquals(
        "Later legacy lock", savedTable.getValue().getPolicies().getLockState().getMessage());
    assertEquals(LockReason.LEGACY, savedTable.getValue().getPolicies().getLockState().getReason());
  }

  @Test
  void legacyUnlockRemovesLegacyLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(LockState.builder().locked(true).reason(LockReason.LEGACY).build())
                    .build())
            .build();

    service.deleteLock("db", "table", "table-admin");

    ArgumentCaptor<TableDto> savedTable = ArgumentCaptor.forClass(TableDto.class);
    verify(service.openHouseInternalRepository).save(savedTable.capture());
    assertNull(savedTable.getValue().getPolicies().getLockState());
  }

  @Test
  void legacyUnlockWithoutActiveLockIsIdempotent() {
    assertDoesNotThrow(() -> service.deleteLock("db", "table", "table-admin"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupLockRequiresTableGeneration() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            service.createLock(
                "db",
                "table",
                CreateUpdateLockRequestBody.builder()
                    .locked(true)
                    .reason(LockReason.TIER3_AUTO_CLEANUP)
                    .build(),
                "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonQualifiedUnlockRejectsLegacyReason() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(LockState.builder().locked(true).reason(LockReason.LEGACY).build())
                    .build())
            .build();

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            service.deleteLock(
                "db", "table", "table-admin", LockReason.LEGACY, "generation", "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonQualifiedUnlockRequiresLockOwner() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            service.deleteLock(
                "db", "table", "table-admin", LockReason.TIER3_AUTO_CLEANUP, "generation", " "));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupLockStoresReasonMessageOwnerAndGeneration() {
    service.createLock("db", "table", cleanupRequest("Cleanup starts tomorrow"), "owner");

    ArgumentCaptor<TableDto> savedTable = ArgumentCaptor.forClass(TableDto.class);
    verify(service.openHouseInternalRepository).save(savedTable.capture());
    assertEquals(
        LockReason.TIER3_AUTO_CLEANUP,
        savedTable.getValue().getPolicies().getLockState().getReason());
    assertEquals(
        "Cleanup starts tomorrow", savedTable.getValue().getPolicies().getLockState().getMessage());
    assertEquals("owner", savedTable.getValue().getPolicies().getLockState().getLockOwner());
    assertEquals("generation", savedTable.getValue().getPolicies().getLockState().getTableUUID());
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "owner", Privileges.LOCK_ADMIN);
    verify(service.authorizationUtils, never())
        .checkTablePrivilege(any(), any(), eq(Privileges.SYSTEM_ADMIN));
  }

  @Test
  void matchingCleanupRetryPreservesOriginalLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(cleanupLock("owner", "Original cleanup message"))
                    .build())
            .build();

    assertDoesNotThrow(
        () -> service.createLock("db", "table", cleanupRequest("Replacement message"), "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupRetryFromAnotherOwnerCannotOverwriteLock() {
    table =
        table
            .toBuilder()
            .policies(Policies.builder().lockState(cleanupLock("other", "Cleanup")).build())
            .build();

    assertThrows(
        AlreadyExistsException.class,
        () -> service.createLock("db", "table", cleanupRequest("Cleanup"), "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyLockCannotReplaceCleanupLock() {
    table =
        table
            .toBuilder()
            .policies(Policies.builder().lockState(cleanupLock("owner", "Cleanup")).build())
            .build();

    assertThrows(
        AlreadyExistsException.class,
        () ->
            service.createLock(
                "db",
                "table",
                CreateUpdateLockRequestBody.builder().locked(true).build(),
                "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupLockCannotReplaceLegacyLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder().lockState(LockState.builder().locked(true).build()).build())
            .build();

    assertThrows(
        AlreadyExistsException.class,
        () -> service.createLock("db", "table", cleanupRequest("Cleanup"), "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void tableAdminCanUnlockMatchingCleanupLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder().lockState(cleanupLock("original-owner", "Cleanup")).build())
            .build();

    assertDoesNotThrow(
        () ->
            service.deleteLock(
                "db",
                "table",
                "table-admin",
                LockReason.TIER3_AUTO_CLEANUP,
                "generation",
                "original-owner"));

    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "table-admin", Privileges.LOCK_ADMIN);
    verify(service.authorizationUtils, never())
        .checkTablePrivilege(any(), any(), eq(Privileges.SYSTEM_ADMIN));
    verify(service.openHouseInternalRepository).save(any());
  }

  @Test
  void legacyUnlockCannotRemoveCleanupLock() {
    table =
        table
            .toBuilder()
            .policies(Policies.builder().lockState(cleanupLock("owner", "Cleanup")).build())
            .build();

    assertThrows(
        AlreadyExistsException.class, () -> service.deleteLock("db", "table", "table-admin"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupUnlockCannotRemoveLegacyLock() {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder().lockState(LockState.builder().locked(true).build()).build())
            .build();

    assertThrows(
        AlreadyExistsException.class,
        () ->
            service.deleteLock(
                "db",
                "table",
                "table-admin",
                LockReason.TIER3_AUTO_CLEANUP,
                "generation",
                "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupUnlockRejectsStaleTableGeneration() {
    table =
        table
            .toBuilder()
            .policies(Policies.builder().lockState(cleanupLock("owner", "Cleanup")).build())
            .build();

    assertThrows(
        AlreadyExistsException.class,
        () ->
            service.deleteLock(
                "db",
                "table",
                "table-admin",
                LockReason.TIER3_AUTO_CLEANUP,
                "stale-generation",
                "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void cleanupUnlockWithoutActiveLockIsIdempotent() {
    assertDoesNotThrow(
        () ->
            service.deleteLock(
                "db",
                "table",
                "table-admin",
                LockReason.TIER3_AUTO_CLEANUP,
                "generation",
                "owner"));

    verify(service.openHouseInternalRepository, never()).save(any());
  }

  private CreateUpdateLockRequestBody cleanupRequest(String message) {
    return CreateUpdateLockRequestBody.builder()
        .locked(true)
        .reason(LockReason.TIER3_AUTO_CLEANUP)
        .message(message)
        .expectedTableUUID("generation")
        .build();
  }

  private LockState cleanupLock(String owner, String message) {
    return LockState.builder()
        .locked(true)
        .reason(LockReason.TIER3_AUTO_CLEANUP)
        .message(message)
        .tableUUID("generation")
        .lockOwner(owner)
        .creationTime(1)
        .build();
  }
}
