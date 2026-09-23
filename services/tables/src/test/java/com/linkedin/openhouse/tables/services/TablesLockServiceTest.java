package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
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
import org.springframework.security.access.AccessDeniedException;

class TablesLockServiceTest {
  private static final String OWNER = "system-only-service";
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
            .tableUri("db.table")
            .tableLocation("metadata-v1")
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    when(service.openHouseInternalRepository.findById(any()))
        .thenAnswer(invocation -> Optional.of(table));
    when(service.openHouseInternalRepository.save(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void systemOnlyUsesExistingLockAuthorizationAndMetadataVersion() {
    service.createLock("db", "table", systemOnlyRequest(), OWNER);
    TableDto saved = savedTable();
    assertEquals(LockReason.SYSTEM_ONLY, saved.getPolicies().getLockState().getReason());
    assertEquals("metadata-v1", saved.getTableVersion());
    verify(service.authorizationUtils).checkLockTablePrivilege(table, OWNER, Privileges.LOCK_ADMIN);
    verifyNoMoreInteractions(service.authorizationUtils);
  }

  @Test
  void matchingSystemOnlyUpdatesExistingLockFields() {
    withLock(systemOnlyLock());
    service.createLock("db", "table", systemOnlyRequest(), OWNER);
    LockState lock = savedTable().getPolicies().getLockState();
    assertEquals("updated message", lock.getMessage());
    assertEquals(456L, lock.getCreationTime());
    assertEquals(3, lock.getExpirationInDays());
  }

  @Test
  void matchingSystemOnlyUpdateUsesExistingAuthorizationForAnotherAdmin() {
    withLock(systemOnlyLock());
    service.createLock("db", "table", systemOnlyRequest(), "another-admin");
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "another-admin", Privileges.LOCK_ADMIN);
    assertEquals("updated message", savedTable().getPolicies().getLockState().getMessage());
  }

  @Test
  void systemOnlyCannotReplaceLegacyLock() {
    withLock(LockState.builder().locked(true).reason(null).build());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyCannotReplaceSystemOnlyLock() {
    withLock(systemOnlyLock());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", legacyRequest(), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyStillUpdatesLegacy() {
    withLock(LockState.builder().locked(true).reason(null).build());
    service.createLock("db", "table", legacyRequest(), OWNER);
    LockState lock = savedTable().getPolicies().getLockState();
    assertEquals("updated legacy", lock.getMessage());
    assertEquals(LockReason.LEGACY, lock.getReason());
  }

  @Test
  void legacyDeleteRefusesSystemOnly() {
    withLock(LockState.builder().locked(true).reason(LockReason.SYSTEM_ONLY).build());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.deleteLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyDeleteStillRemovesNullReasonLock() {
    withLock(LockState.builder().locked(true).reason(null).build());
    service.deleteLock("db", "table", OWNER);
    assertNull(savedTable().getPolicies().getLockState());
  }

  @Test
  void legacyDeleteWithoutActiveLockIsIdempotent() {
    service.deleteLock("db", "table", OWNER);
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void lockAdministrationStillRequiresLockAdmin() {
    doThrow(new AccessDeniedException("denied"))
        .when(service.authorizationUtils)
        .checkLockTablePrivilege(table, OWNER, Privileges.LOCK_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(), OWNER));
    assertThrows(AccessDeniedException.class, () -> service.deleteLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void replicaLockAdministrationRemainsForbidden() {
    table = table.toBuilder().tableType(TableType.REPLICA_TABLE).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(), OWNER));
    assertThrows(
        UnsupportedOperationException.class, () -> service.deleteLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void missingTableStillReturnsNotFound() {
    when(service.openHouseInternalRepository.findById(any())).thenReturn(Optional.empty());
    assertThrows(
        NoSuchUserTableException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(), OWNER));
    assertThrows(NoSuchUserTableException.class, () -> service.deleteLock("db", "table", OWNER));
    assertThrows(
        NoSuchUserTableException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, OWNER));
    assertThrows(NoSuchUserTableException.class, () -> service.getLock("db", "table", OWNER));
  }

  @Test
  void reasonTargetedUnlockRejectsMismatchedReason() {
    withLock(systemOnlyLock());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.deleteLock("db", "table", LockReason.LEGACY, OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonTargetedUnlockIsANoOpWhenInactive() {
    withLock(LockState.builder().locked(false).reason(LockReason.SYSTEM_ONLY).build());
    service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, OWNER);
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonTargetedUnlockUsesExistingAuthorizationAndMetadataVersion() {
    withLock(systemOnlyLock());
    service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, "another-admin");
    TableDto saved = savedTable();
    assertNull(saved.getPolicies().getLockState());
    assertEquals(table.getTableLocation(), saved.getTableVersion());
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "another-admin", Privileges.LOCK_ADMIN);
    verifyNoMoreInteractions(service.authorizationUtils);
  }

  @Test
  void reasonTargetedUnlockRequiresAReason() {
    withLock(systemOnlyLock());
    assertThrows(
        RequestValidationFailureException.class,
        () -> service.deleteLock("db", "table", null, OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonTargetedUnlockAndStatusPreserveAuthorizationChecks() {
    withLock(systemOnlyLock());
    doThrow(new AccessDeniedException("denied"))
        .when(service.authorizationUtils)
        .checkLockTablePrivilege(table, OWNER, Privileges.LOCK_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, OWNER));
    assertEquals(
        table.getPolicies().getLockState(), service.getLock("db", "table", OWNER).getLockState());
    verify(service.authorizationUtils)
        .checkTablePrivilege(table, OWNER, Privileges.GET_TABLE_METADATA);
    doThrow(new AccessDeniedException("denied"))
        .when(service.authorizationUtils)
        .checkTablePrivilege(table, OWNER, Privileges.GET_TABLE_METADATA);
    assertThrows(AccessDeniedException.class, () -> service.getLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void reasonTargetedUnlockStillRefusesReplicas() {
    withLock(systemOnlyLock());
    table = table.toBuilder().tableType(TableType.REPLICA_TABLE).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  private CreateUpdateLockRequestBody systemOnlyRequest() {
    return CreateUpdateLockRequestBody.builder()
        .locked(true)
        .reason(LockReason.SYSTEM_ONLY)
        .message("updated message")
        .creationTime(456L)
        .expirationInDays(3)
        .build();
  }

  private CreateUpdateLockRequestBody legacyRequest() {
    return CreateUpdateLockRequestBody.builder().locked(true).message("updated legacy").build();
  }

  private LockState systemOnlyLock() {
    return LockState.builder()
        .locked(true)
        .reason(LockReason.SYSTEM_ONLY)
        .message("original message")
        .creationTime(123L)
        .build();
  }

  private void withLock(LockState lock) {
    table = table.toBuilder().policies(Policies.builder().lockState(lock).build()).build();
  }

  private TableDto savedTable() {
    ArgumentCaptor<TableDto> captor = ArgumentCaptor.forClass(TableDto.class);
    verify(service.openHouseInternalRepository).save(captor.capture());
    return captor.getValue();
  }
}
