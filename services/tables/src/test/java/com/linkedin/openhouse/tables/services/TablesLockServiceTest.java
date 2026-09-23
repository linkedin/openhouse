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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.security.access.AccessDeniedException;

class TablesLockServiceTest {
  private static final String UUID = "current-generation";
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
            .tableUUID(UUID)
            .tableUri("db.table")
            .tableLocation("metadata-v1")
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    when(service.openHouseInternalRepository.findById(any()))
        .thenAnswer(invocation -> Optional.of(table));
    when(service.openHouseInternalRepository.save(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void systemOnlyRequiresNonblankGeneration(String generation) {
    assertThrows(
        RequestValidationFailureException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(generation), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void systemOnlyRejectsStaleGeneration() {
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest("previous-generation"), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void systemOnlyRecordsActingPrincipalAndCurrentGeneration() {
    service.createLock("db", "table", systemOnlyRequest(UUID), OWNER);
    TableDto saved = savedTable();
    assertEquals(OWNER, saved.getPolicies().getLockState().getLockOwner());
    assertEquals(UUID, saved.getPolicies().getLockState().getTableUUID());
    assertEquals("metadata-v1", saved.getTableVersion());
    verify(service.authorizationUtils).checkLockTablePrivilege(table, OWNER, Privileges.LOCK_ADMIN);
    verifyNoMoreInteractions(service.authorizationUtils);
  }

  @Test
  void matchingSystemOnlyRetryDoesNotRewriteMetadata() {
    withLock(systemOnlyLock(OWNER, UUID));
    service.createLock("db", "table", systemOnlyRequest(UUID), OWNER);
    verify(service.openHouseInternalRepository, never()).save(any());
    assertEquals("original message", table.getPolicies().getLockState().getMessage());
    assertEquals(123L, table.getPolicies().getLockState().getCreationTime());
  }

  @Test
  void differentSystemOnlyOwnerCannotReplaceActiveLock() {
    withLock(systemOnlyLock("other-owner", UUID));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void differentStoredGenerationCannotReplaceActiveLock() {
    withLock(systemOnlyLock(OWNER, "previous-generation"));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void systemOnlyCannotReplaceLegacyLock() {
    withLock(LockState.builder().locked(true).reason(null).build());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyCannotReplaceSystemOnlyLock() {
    withLock(systemOnlyLock(OWNER, UUID));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", legacyRequest(), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void legacyStillUpdatesLegacyWithoutGeneration() {
    withLock(LockState.builder().locked(true).reason(null).build());
    service.createLock("db", "table", legacyRequest(), OWNER);
    LockState lock = savedTable().getPolicies().getLockState();
    assertEquals("updated legacy", lock.getMessage());
    assertEquals(LockReason.LEGACY, lock.getReason());
    assertNull(lock.getLockOwner());
    assertNull(lock.getTableUUID());
  }

  @Test
  void legacyDeleteRefusesSystemOnlyIncludingPreOwnerLocks() {
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
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    assertThrows(AccessDeniedException.class, () -> service.deleteLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void replicaLockAdministrationRemainsForbidden() {
    table = table.toBuilder().tableType(TableType.REPLICA_TABLE).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    assertThrows(
        UnsupportedOperationException.class, () -> service.deleteLock("db", "table", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void missingTableStillReturnsNotFound() {
    when(service.openHouseInternalRepository.findById(any())).thenReturn(Optional.empty());
    assertThrows(
        NoSuchUserTableException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    assertThrows(NoSuchUserTableException.class, () -> service.deleteLock("db", "table", OWNER));
    assertThrows(
        NoSuchUserTableException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, OWNER));
    assertThrows(NoSuchUserTableException.class, () -> service.getLock("db", "table", OWNER));
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "other-owner,current-generation",
        "system-only-service,old-generation",
        "NULL,current-generation",
        "system-only-service,NULL",
        "NULL,NULL"
      },
      nullValues = "NULL")
  void guardedUnlockRejectsStoredIdentityMismatch(String recordedOwner, String generation) {
    withLock(systemOnlyLock(recordedOwner, generation));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, OWNER));
    assertThrows(
        EntityConcurrentModificationException.class,
        () ->
            service.deleteLock(
                "db", "table", LockReason.SYSTEM_ONLY, UUID, "__UNRECORDED__", OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void guardedUnlockChecksCurrentGenerationBeforeInactiveRetry() {
    withLock(LockState.builder().locked(false).reason(LockReason.SYSTEM_ONLY).build());
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, "old", OWNER, OWNER));
    service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, OWNER);
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void guardedUnlockUsesRecordedOwnerNotActingAdminAndExistingMetadataVersion() {
    withLock(systemOnlyLock(OWNER, UUID));
    service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, "another-admin");
    TableDto saved = savedTable();
    assertNull(saved.getPolicies().getLockState());
    assertEquals(table.getTableLocation(), saved.getTableVersion());
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(table, "another-admin", Privileges.LOCK_ADMIN);
    verifyNoMoreInteractions(service.authorizationUtils);
  }

  @Test
  void preOwnerSystemOnlyCannotBeClaimedByACreateRetry() {
    withLock(systemOnlyLock(null, null));
    assertThrows(
        EntityConcurrentModificationException.class,
        () -> service.createLock("db", "table", systemOnlyRequest(UUID), OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void guardedUnlockAndStatusPreserveAuthorizationChecks() {
    withLock(systemOnlyLock(OWNER, UUID));
    doThrow(new AccessDeniedException("denied"))
        .when(service.authorizationUtils)
        .checkLockTablePrivilege(table, OWNER, Privileges.LOCK_ADMIN);
    assertThrows(
        AccessDeniedException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, OWNER));
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
  void guardedUnlockStillRefusesReplicas() {
    withLock(systemOnlyLock(OWNER, UUID));
    table = table.toBuilder().tableType(TableType.REPLICA_TABLE).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> service.deleteLock("db", "table", LockReason.SYSTEM_ONLY, UUID, OWNER, OWNER));
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  private CreateUpdateLockRequestBody systemOnlyRequest(String generation) {
    return CreateUpdateLockRequestBody.builder()
        .locked(true)
        .reason(LockReason.SYSTEM_ONLY)
        .expectedTableUUID(generation)
        .message("retry message")
        .creationTime(456L)
        .build();
  }

  private CreateUpdateLockRequestBody legacyRequest() {
    return CreateUpdateLockRequestBody.builder().locked(true).message("updated legacy").build();
  }

  private LockState systemOnlyLock(String owner, String generation) {
    return LockState.builder()
        .locked(true)
        .reason(LockReason.SYSTEM_ONLY)
        .lockOwner(owner)
        .tableUUID(generation)
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
