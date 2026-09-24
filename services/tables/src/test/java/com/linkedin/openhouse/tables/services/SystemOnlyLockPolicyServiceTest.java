package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class SystemOnlyLockPolicyServiceTest {
  private OpenHouseInternalRepository repository;
  private TablesServiceImpl tables;
  private IcebergSnapshotsServiceImpl snapshots;
  private TableDto current;
  private LockState systemOnly;

  @AfterEach
  void clearRequest() {
    RequestContextHolder.resetRequestAttributes();
  }

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
  @CsvSource({
    "update,reason",
    "snapshot,reason",
    "snapshot,locked",
    "snapshotReplace,reason",
    "snapshotReplace,locked",
    "stagedReplace,reason",
    "stagedReplace,locked"
  })
  void systemActionCannotChangeSystemOnlyLockFlagOrReason(String operation, String field) {
    enableSystemAction();
    LockState changed =
        "reason".equals(field)
            ? LockState.builder().locked(true).reason(LockReason.LEGACY).build()
            : LockState.builder().locked(false).build();
    assertThrows(
        RequestValidationFailureException.class,
        () -> write(operation, request(Policies.builder().lockState(changed).build())));
    verify(repository, never()).save(any());
  }

  @ParameterizedTest
  @CsvSource({
    "update,policies",
    "update,lock",
    "update,message",
    "snapshot,policies",
    "snapshot,lock",
    "snapshot,message",
    "stagedReplace,policies",
    "stagedReplace,lock",
    "stagedReplace,message"
  })
  void systemActionWritesPreserveTheExistingLock(String operation, String omitted) {
    enableSystemAction();
    Policies policies;
    switch (omitted) {
      case "policies":
        policies = null;
        break;
      case "lock":
        policies = Policies.builder().sharingEnabled(true).build();
        break;
      default:
        policies =
            Policies.builder()
                .lockState(LockState.builder().locked(true).reason(LockReason.SYSTEM_ONLY).build())
                .build();
    }
    write(operation, request(policies));
    assertEquals(systemOnly, saved().getPolicies().getLockState());
  }

  @ParameterizedTest
  @ValueSource(strings = {"update", "snapshot"})
  void inactiveSystemOnlyDoesNotAddPolicyPreservationRules(String operation) {
    systemOnly = LockState.builder().locked(false).reason(LockReason.SYSTEM_ONLY).build();
    current =
        current
            .toBuilder()
            .policies(current.getPolicies().toBuilder().lockState(systemOnly).build())
            .build();
    write(operation, request(Policies.builder().sharingEnabled(true).build()));
    assertNull(saved().getPolicies().getLockState());
  }

  private CreateUpdateTableRequestBody request(Policies policies) {
    return CreateUpdateTableRequestBody.builder()
        .databaseId("db")
        .tableId("table")
        .clusterId("cluster")
        .baseTableVersion("v1")
        .tableProperties(Collections.singletonMap("updated", "value"))
        .policies(policies)
        .build();
  }

  private void enableSystemAction() {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(TablesMvcConstants.HTTP_HEADER_ACTION_TYPE, "SYSTEM");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  private void write(String operation, CreateUpdateTableRequestBody request) {
    switch (operation) {
      case "update":
        tables.putTable(request, "owner", false);
        break;
      case "stagedReplace":
        tables.putTable(request.toBuilder().stageReplace(true).build(), "owner", true);
        break;
      default:
        snapshots.putIcebergSnapshots(
            "db",
            "table",
            IcebergSnapshotsRequestBody.builder()
                .createUpdateTableRequestBody(
                    request.toBuilder().replaceCommit("snapshotReplace".equals(operation)).build())
                .baseTableVersion("v1")
                .build(),
            "owner");
    }
  }

  private TableDto saved() {
    ArgumentCaptor<TableDto> captor = ArgumentCaptor.forClass(TableDto.class);
    verify(repository).save(captor.capture());
    return captor.getValue();
  }
}
