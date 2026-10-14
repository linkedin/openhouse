package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.controller.TablesController;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class GuardedSystemLockTest {
  private static final String PATH = "/v1/databases/db/tables/table/lock/SYSTEM_ONLY/guarded";
  private static final String UUID = "73ea0d21-3c89-4987-a6cf-26e4f86bdcee";
  private static final String OWNER = "automation";
  private final ObjectMapper mapper = new ObjectMapper();
  private TablesServiceImpl service;
  private TableDto table;
  private MockMvc mvc;

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
            .tableUUID(UUID)
            .tableLocation("metadata-v1")
            .tableType(TableType.PRIMARY_TABLE)
            .build();
    when(service.openHouseInternalRepository.findById(any()))
        .thenAnswer(invocation -> Optional.of(table));
    when(service.openHouseInternalRepository.save(any()))
        .thenAnswer(
            invocation -> {
              table = invocation.getArgument(0);
              return table;
            });
    OpenHouseTablesApiHandler handler = new OpenHouseTablesApiHandler();
    ReflectionTestUtils.setField(handler, "tableService", service);
    ReflectionTestUtils.setField(handler, "tablesApiValidator", mock(TablesApiValidator.class));
    TablesController controller = new TablesController();
    ReflectionTestUtils.setField(controller, "tablesApiHandler", handler);
    mvc =
        MockMvcBuilders.standaloneSetup(controller)
            .setControllerAdvice(new OpenHouseExceptionHandler())
            .build();
    authenticate(OWNER);
  }

  @AfterEach
  void clearAuthentication() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void guardedCreateRecordsIdentityAndPinsGeneration() throws Exception {
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    JsonNode lock = mapper.valueToTree(table.getPolicies().getLockState());
    assertEquals("SYSTEM_ONLY", lock.path("reason").asText());
    assertEquals(OWNER, lock.path("lockOwner").asText());
    assertEquals(UUID, lock.path("tableUUID").asText());
    assertEquals("cleanup", lock.path("lockId").asText());
    assertTrue(lock.path("locked").asBoolean());
    assertEquals("metadata-v1", table.getTableVersion());
    PoliciesSpecMapper policiesMapper = new PoliciesSpecMapper();
    assertEquals(
        table.getPolicies(),
        policiesMapper.toPoliciesObject(policiesMapper.toPoliciesJsonString(table)));
    verify(service.authorizationUtils)
        .checkLockTablePrivilege(any(), eq(OWNER), eq(Privileges.LOCK_ADMIN));
  }

  @Test
  void ordinaryPolicyInputCannotForgeGuardedIdentity() {
    LockState parsed =
        assertDoesNotThrow(
            () ->
                mapper.readValue(
                    "{\"locked\":true,\"reason\":\"SYSTEM_ONLY\","
                        + "\"lockOwner\":\"forged\",\"tableUUID\":\"forged\",\"lockId\":\"forged\"}",
                    LockState.class));
    JsonNode serialized = mapper.valueToTree(parsed);
    assertTrue(serialized.path("lockOwner").isNull());
    assertTrue(serialized.path("tableUUID").isNull());
    assertTrue(serialized.path("lockId").isNull());
  }

  @Test
  void retryPreservesTheExistingLock() throws Exception {
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    LockState original = table.getPolicies().getLockState();
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    assertSame(original, table.getPolicies().getLockState());
    verify(service.openHouseInternalRepository, times(1)).save(any());
  }

  @Test
  void recreatedTableCannotBeLocked() throws Exception {
    mvc.perform(create("older-generation", "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void anotherWorkflowCannotAdoptTheSameOwnersLock() throws Exception {
    mvc.perform(create(UUID, "another-workflow")).andExpect(status().isCreated());
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, times(1)).save(any());
  }

  @Test
  void anotherPrincipalCannotAdoptTheLock() throws Exception {
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    authenticate("another-owner");
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, times(1)).save(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"LEGACY", "SYSTEM_ONLY"})
  void unguardedLocksAreNeverAdopted(String reason) throws Exception {
    table =
        table
            .toBuilder()
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder().locked(true).reason(LockReason.valueOf(reason)).build())
                    .build())
            .build();
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isConflict());
    mvc.perform(unlock(UUID, OWNER, "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void authorizedAdminCanUnlockOnlyTheRecordedIdentity() throws Exception {
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    authenticate("another-admin");
    mvc.perform(unlock(UUID, OWNER, "cleanup")).andExpect(status().isNoContent());
    assertNull(table.getPolicies().getLockState());
    mvc.perform(unlock(UUID, OWNER, "cleanup")).andExpect(status().isNoContent());
    verify(service.openHouseInternalRepository, times(2)).save(any());
  }

  @Test
  void unlockRefusesChangedGenerationOwnerOrWorkflow() throws Exception {
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isCreated());
    mvc.perform(unlock("older-generation", OWNER, "cleanup")).andExpect(status().isConflict());
    mvc.perform(unlock(UUID, "another-owner", "cleanup")).andExpect(status().isConflict());
    mvc.perform(unlock(UUID, OWNER, "another-workflow")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, times(1)).save(any());
  }

  @Test
  void inactiveLockStillChecksGeneration() throws Exception {
    mvc.perform(unlock("older-generation", OWNER, "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void authorizationIsCheckedBeforeGeneration() throws Exception {
    doThrow(new AccessDeniedException("denied"))
        .when(service.authorizationUtils)
        .checkLockTablePrivilege(any(), eq(OWNER), eq(Privileges.LOCK_ADMIN));
    mvc.perform(create("older-generation", "cleanup")).andExpect(status().isForbidden());
    mvc.perform(unlock("older-generation", OWNER, "cleanup")).andExpect(status().isForbidden());
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void requiredGuardsCannotBeOmittedOrBlank() throws Exception {
    mvc.perform(post(PATH).contentType(MediaType.APPLICATION_JSON).content("{}"))
        .andExpect(status().isBadRequest());
    mvc.perform(create(" ", "cleanup")).andExpect(status().isBadRequest());
    mvc.perform(create(UUID, " ")).andExpect(status().isBadRequest());
    mvc.perform(delete(PATH)).andExpect(status().isBadRequest());
    mvc.perform(unlock(UUID, "", "cleanup")).andExpect(status().isBadRequest());
    verify(service.openHouseInternalRepository, never()).save(any());
  }

  @Test
  void concurrentMetadataWriteIsNotRetriedAgainstANewerGeneration() throws Exception {
    doThrow(
            new EntityConcurrentModificationException(
                "db.table", "changed", new IllegalStateException("changed")))
        .when(service.openHouseInternalRepository)
        .save(any());
    mvc.perform(create(UUID, "cleanup")).andExpect(status().isConflict());
    verify(service.openHouseInternalRepository, times(1)).findById(any());
    verify(service.openHouseInternalRepository, times(1)).save(any());
  }

  private MockHttpServletRequestBuilder create(String generation, String lockId) throws Exception {
    return post(PATH)
        .contentType(MediaType.APPLICATION_JSON)
        .content(
            mapper
                .createObjectNode()
                .put("expectedTableUUID", generation)
                .put("lockId", lockId)
                .put("message", "maintenance workflow")
                .toString());
  }

  private MockHttpServletRequestBuilder unlock(String generation, String owner, String lockId) {
    return delete(PATH)
        .param("expectedTableUUID", generation)
        .param("lockOwner", owner)
        .param("lockId", lockId);
  }

  private void authenticate(String principal) {
    SecurityContextHolder.getContext()
        .setAuthentication(
            new UsernamePasswordAuthenticationToken(
                new User(principal, "", Collections.emptyList()), "", Collections.emptyList()));
  }
}
