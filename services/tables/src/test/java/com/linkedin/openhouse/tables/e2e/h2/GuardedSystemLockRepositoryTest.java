package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.TABLE_DTO;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateGuardedLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.impl.TablePolicyManager;
import com.linkedin.openhouse.tables.services.TablesService;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class GuardedSystemLockRepositoryTest {
  @Autowired private OpenHouseInternalRepository repository;
  @Autowired private TablesService service;
  @Autowired private Catalog catalog;
  @SpyBean private TablePolicyManager policyManager;
  @MockBean private AuthorizationUtils authorizationUtils;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void concurrentLockSurvivesGuardedCommit(boolean deleting) {
    String tableId = deleting ? "guarded_delete_race" : "guarded_create_race";
    TableIdentifier identifier = TableIdentifier.of(TABLE_DTO.getDatabaseId(), tableId);
    TableDto table =
        repository.save(
            TABLE_DTO
                .toBuilder()
                .tableId(tableId)
                .tableUri(
                    TABLE_DTO.getClusterId() + "." + TABLE_DTO.getDatabaseId() + "." + tableId)
                .tableVersion(INITIAL_TABLE_VERSION)
                .build());
    CreateGuardedLockRequestBody request =
        CreateGuardedLockRequestBody.builder()
            .expectedTableUUID(table.getTableUUID())
            .lockId("cleanup")
            .build();
    try {
      if (deleting) {
        service.createGuardedSystemLock(table.getDatabaseId(), tableId, request, "cleanup-owner");
      }
      LockState competingLock =
          LockState.builder()
              .locked(true)
              .reason(LockReason.SYSTEM_ONLY)
              .tableUUID(table.getTableUUID())
              .lockOwner("another-owner")
              .lockId("another-workflow")
              .build();
      TableDto competingTable =
          table.toBuilder().policies(Policies.builder().lockState(competingLock).build()).build();
      doAnswer(
              invocation -> {
                // This hook runs after the repository's version check, before transaction commit.
                // No COMMIT_KEY: the writer must not benefit from the JVM-local retry cache.
                Table concurrentWriter = catalog.loadTable(identifier);
                concurrentWriter
                    .updateProperties()
                    .set("policies", new PoliciesSpecMapper().toPoliciesJsonString(competingTable))
                    .commit();
                return invocation.callRealMethod();
              })
          .when(policyManager)
          .managePoliciesOnUpdateIfNeeded(any(), any(), anyMap());

      assertThrows(
          EntityConcurrentModificationException.class,
          () -> {
            if (deleting) {
              service.deleteGuardedSystemLock(
                  table.getDatabaseId(),
                  tableId,
                  table.getTableUUID(),
                  "cleanup-owner",
                  "cleanup",
                  "cleanup-owner");
            } else {
              service.createGuardedSystemLock(
                  table.getDatabaseId(), tableId, request, "cleanup-owner");
            }
          });
      TableDto persisted =
          repository
              .findById(
                  TableDtoPrimaryKey.builder()
                      .databaseId(table.getDatabaseId())
                      .tableId(tableId)
                      .build())
              .orElseThrow(AssertionError::new);
      assertEquals(competingLock, persisted.getPolicies().getLockState());
      verify(policyManager, times(deleting ? 2 : 1))
          .managePoliciesOnUpdateIfNeeded(any(), any(), anyMap());
    } finally {
      catalog.dropTable(identifier);
    }
  }
}
