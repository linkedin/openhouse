package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

class SystemOnlyLockPolicyServiceTest {
  private OpenHouseInternalRepository repository;
  private TablesServiceImpl tables;
  private IcebergSnapshotsServiceImpl snapshots;
  private TableDto current;
  private LockState systemOnly;

  @BeforeEach
  void setUp() throws Exception {
    repository = mock(OpenHouseInternalRepository.class);
    TablesMapper mapper = Mappers.getMapper(TablesMapper.class);
    ReflectionTestUtils.setField(mapper, "policiesSpecMapper", new PoliciesSpecMapper());
    AuthorizationUtils authorization = mock(AuthorizationUtils.class);
    TableUUIDGenerator generator = mock(TableUUIDGenerator.class);
    when(generator.generateUUID(any(CreateUpdateTableRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    when(generator.generateUUID(any(IcebergSnapshotsRequestBody.class)))
        .thenReturn(UUID.randomUUID());
    ReadBridgeStripProtection protection = mock(ReadBridgeStripProtection.class);
    when(protection.prepare(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    tables = new TablesServiceImpl();
    tables.openHouseInternalRepository = repository;
    tables.tablesMapper = mapper;
    tables.authorizationUtils = authorization;
    tables.tableUUIDGenerator = generator;
    tables.readBridgeStripProtection = protection;
    snapshots = new IcebergSnapshotsServiceImpl();
    snapshots.openHouseInternalRepository = repository;
    snapshots.tablesMapper = mapper;
    snapshots.authorizationUtils = authorization;
    snapshots.tableUUIDGenerator = generator;
    snapshots.readBridgeStripProtection = protection;
    systemOnly =
        LockState.builder()
            .locked(true)
            .reason(LockReason.SYSTEM_ONLY)
            .lockOwner("owner")
            .tableUUID("uuid")
            .message("original")
            .creationTime(123)
            .build();
    current =
        TableDto.builder()
            .databaseId("db")
            .tableId("table")
            .clusterId("cluster")
            .tableUUID("uuid")
            .tableLocation("v1")
            .policies(
                Policies.builder()
                    .lockState(systemOnly)
                    .retention(Retention.builder().count(3).build())
                    .sharingEnabled(true)
                    .build())
            .build();
    when(repository.findById(any())).thenAnswer(invocation -> Optional.ofNullable(current));
    when(repository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void genericCreateCannotSmuggleSystemOnly(boolean snapshotWrite, boolean staged) {
    current = null;
    CreateUpdateTableRequestBody request =
        request(Policies.builder().lockState(systemOnly).build())
            .toBuilder()
            .stageCreate(staged)
            .build();
    assertThrows(RequestValidationFailureException.class, () -> write(snapshotWrite, request));
    verify(repository, never()).save(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"reason", "owner", "generation", "message", "time", "locked"})
  void stagedReplaceCannotChangeAnySystemOnlyLockField(String field) {
    LockState changed;
    switch (field) {
      case "reason":
        changed = LockState.builder().locked(true).reason(LockReason.LEGACY).build();
        break;
      case "owner":
        changed =
            LockState.builder()
                .locked(true)
                .reason(LockReason.SYSTEM_ONLY)
                .lockOwner("other")
                .tableUUID("uuid")
                .message("original")
                .creationTime(123)
                .build();
        break;
      case "generation":
        changed =
            LockState.builder()
                .locked(true)
                .reason(LockReason.SYSTEM_ONLY)
                .lockOwner("owner")
                .tableUUID("other")
                .message("original")
                .creationTime(123)
                .build();
        break;
      case "message":
        changed =
            LockState.builder()
                .locked(true)
                .reason(LockReason.SYSTEM_ONLY)
                .lockOwner("owner")
                .tableUUID("uuid")
                .message("changed")
                .creationTime(123)
                .build();
        break;
      case "time":
        changed =
            LockState.builder()
                .locked(true)
                .reason(LockReason.SYSTEM_ONLY)
                .lockOwner("owner")
                .tableUUID("uuid")
                .message("original")
                .creationTime(456)
                .build();
        break;
      default:
        changed =
            LockState.builder()
                .locked(false)
                .reason(LockReason.SYSTEM_ONLY)
                .lockOwner("owner")
                .tableUUID("uuid")
                .message("original")
                .creationTime(123)
                .build();
    }
    CreateUpdateTableRequestBody request =
        request(Policies.builder().lockState(changed).build())
            .toBuilder()
            .stageReplace(true)
            .build();
    assertThrows(RequestValidationFailureException.class, () -> write(false, request));
    verify(repository, never()).save(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stagedReplacePreservesSystemOnlyWhenPoliciesOrLockOmitted(boolean omitPolicies) {
    Policies policies = omitPolicies ? null : Policies.builder().sharingEnabled(true).build();
    write(false, request(policies).toBuilder().stageReplace(true).build());
    Policies saved = saved().getPolicies();
    assertEquals(systemOnly, saved.getLockState());
    assertTrue(saved.isSharingEnabled());
    if (omitPolicies) {
      assertEquals(current.getPolicies().getRetention(), saved.getRetention());
    }
  }

  @Test
  void stagedReplaceAcceptsAnExactlyUnchangedSystemOnlyState() {
    write(false, request(current.getPolicies()).toBuilder().stageReplace(true).build());
    assertEquals(systemOnly, saved().getPolicies().getLockState());
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void ordinaryWritesPreserveOmittedInactiveSystemOnlyMetadata(
      boolean snapshotWrite, boolean omitPolicies) {
    systemOnly =
        LockState.builder()
            .locked(false)
            .reason(LockReason.SYSTEM_ONLY)
            .lockOwner("owner")
            .tableUUID("uuid")
            .build();
    current =
        current
            .toBuilder()
            .policies(current.getPolicies().toBuilder().lockState(systemOnly).build())
            .build();
    write(
        snapshotWrite,
        request(omitPolicies ? null : Policies.builder().sharingEnabled(true).build()));
    assertEquals(systemOnly, saved().getPolicies().getLockState());
    if (omitPolicies) {
      assertEquals(current.getPolicies(), saved().getPolicies());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void snapshotUpdatesAndReplaceCommitsCannotMutateSystemOnly(boolean replace) {
    CreateUpdateTableRequestBody request =
        request(
                Policies.builder()
                    .lockState(LockState.builder().locked(true).reason(LockReason.LEGACY).build())
                    .build())
            .toBuilder()
            .replaceCommit(replace)
            .build();
    assertThrows(RequestValidationFailureException.class, () -> write(true, request));
    verify(repository, never()).save(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void systemOnlyCannotBeIntroducedThroughUpdate(boolean snapshotWrite) {
    current = current.toBuilder().policies(null).build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> write(snapshotWrite, request(Policies.builder().lockState(systemOnly).build())));
    verify(repository, never()).save(any());
  }

  @Test
  void unchangedActiveSystemOnlyStillUsesExistingSnapshotWriteDenial() {
    assertThrows(
        UnsupportedClientOperationException.class,
        () -> write(true, request(null).toBuilder().replaceCommit(true).build()));
    verify(repository, never()).save(any());
  }

  private CreateUpdateTableRequestBody request(Policies policies) {
    return CreateUpdateTableRequestBody.builder()
        .databaseId("db")
        .tableId("table")
        .clusterId("cluster")
        .baseTableVersion("v1")
        .policies(policies)
        .build();
  }

  private void write(boolean snapshotWrite, CreateUpdateTableRequestBody request) {
    if (snapshotWrite) {
      snapshots.putIcebergSnapshots(
          "db",
          "table",
          IcebergSnapshotsRequestBody.builder()
              .createUpdateTableRequestBody(request)
              .baseTableVersion("v1")
              .build(),
          "owner");
    } else {
      tables.putTable(request, "owner", false);
    }
  }

  private TableDto saved() {
    ArgumentCaptor<TableDto> captor = ArgumentCaptor.forClass(TableDto.class);
    verify(repository).save(captor.capture());
    return captor.getValue();
  }
}
