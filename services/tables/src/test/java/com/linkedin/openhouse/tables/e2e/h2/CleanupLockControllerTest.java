package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.config.TablesMvcConstants.HTTP_HEADER_SYSTEM_ACTION;
import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor.DummySecurityJWT;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@SpringBootTest(classes = SpringH2Application.class)
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
class CleanupLockControllerTest {
  private static final String DB = GET_TABLE_RESPONSE_BODY.getDatabaseId();
  private static final String TABLE = "cleanup_lock_lifecycle";
  private static final String OWNER = "lock-owner";
  private static final String PATH = "/v1/databases/" + DB + "/tables/" + TABLE;
  private static final String CLEANUP_PATH = PATH + "/lock/TIER3_AUTO_CLEANUP";
  private static final TableDtoPrimaryKey KEY =
      TableDtoPrimaryKey.builder().databaseId(DB).tableId(TABLE).build();

  @Autowired private MockMvc mvc;
  @Autowired private OpenHouseInternalRepository repository;
  @MockBean private AuthorizationUtils authorizationUtils;
  private String tableUUID;

  @BeforeEach
  void createTable() throws Exception {
    Map<String, String> properties = new HashMap<>(GET_TABLE_RESPONSE_BODY.getTableProperties());
    properties.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
    String body =
        mvc.perform(
                auth(post("/v1/databases/" + DB + "/tables"))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        buildCreateUpdateTableRequestBody(
                                GET_TABLE_RESPONSE_BODY
                                    .toBuilder()
                                    .tableId(TABLE)
                                    .tableProperties(properties)
                                    .policies(
                                        GET_TABLE_RESPONSE_BODY
                                            .getPolicies()
                                            .toBuilder()
                                            .replication(null)
                                            .build())
                                    .build())
                            .toJson()))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();
    tableUUID = JsonPath.read(body, "$.tableUUID");
  }

  @AfterEach
  void deleteTable() {
    repository.deleteById(KEY);
  }

  @Test
  void statusIsMetadataOnlyAndDoesNotRequireLockAdmin() throws Exception {
    createCleanup();
    doThrow(new AccessDeniedException("no lock admin"))
        .when(authorizationUtils)
        .checkLockTablePrivilege(any(), any(), any());
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$", aMapWithSize(2)))
        .andExpect(jsonPath("$.tableUUID").value(tableUUID))
        .andExpect(jsonPath("$.lockState.locked").value(true))
        .andExpect(jsonPath("$.lockState.reason").value("TIER3_AUTO_CLEANUP"))
        .andExpect(jsonPath("$.lockState.lockOwner").value(OWNER))
        .andExpect(jsonPath("$.lockState.tableUUID").value(tableUUID))
        .andExpect(jsonPath("$.tableLocation").doesNotExist())
        .andExpect(jsonPath("$.schema").doesNotExist());
    verify(authorizationUtils)
        .checkTablePrivilege(any(), eq(OWNER), eq(Privileges.GET_TABLE_METADATA));
  }

  @Test
  void inactiveStatusIsNullAndMissingTableIsNotFound() throws Exception {
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tableUUID").value(tableUUID))
        .andExpect(jsonPath("$.lockState").value(nullValue()));
    storeLock(LockState.builder().locked(false).build());
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.lockState").value(nullValue()));
    mvc.perform(auth(get(PATH + "_missing/lock"))).andExpect(status().isNotFound());
  }

  @Test
  void statusPreservesLegacyDefaults() throws Exception {
    storeLock(LockState.builder().locked(true).reason(null).build());
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.lockState.reason").value("LEGACY"))
        .andExpect(jsonPath("$.lockState.message").value("Default"))
        .andExpect(jsonPath("$.lockState.lockOwner").value(nullValue()));
  }

  @Test
  void statusAndUnlockKeepExistingAuthorization() throws Exception {
    createCleanup();
    doThrow(new AccessDeniedException("denied"))
        .when(authorizationUtils)
        .checkTablePrivilege(any(), eq(OWNER), eq(Privileges.GET_TABLE_METADATA));
    mvc.perform(auth(get(PATH + "/lock"))).andExpect(status().isForbidden());
    doThrow(new AccessDeniedException("denied"))
        .when(authorizationUtils)
        .checkLockTablePrivilege(any(), eq(OWNER), eq(Privileges.LOCK_ADMIN));
    mvc.perform(unlock(tableUUID, OWNER)).andExpect(status().isForbidden());
  }

  @Test
  void guardedUnlockWorksForAnotherLockAdminAndRetriesAreNoOps() throws Exception {
    createCleanup();
    mvc.perform(auth(delete(PATH + "/lock"))).andExpect(status().isConflict());
    mvc.perform(
            delete(CLEANUP_PATH)
                .param("expectedTableUUID", tableUUID)
                .param("expectedLockOwner", OWNER)
                .header(
                    "Authorization",
                    "Bearer " + new DummySecurityJWT("another-admin").buildNoopJWT()))
        .andExpect(status().isNoContent());
    String location = repository.findById(KEY).get().getTableLocation();
    mvc.perform(unlock(tableUUID, OWNER)).andExpect(status().isNoContent());
    org.junit.jupiter.api.Assertions.assertEquals(
        location, repository.findById(KEY).get().getTableLocation());
    mvc.perform(unlock("previous-generation", OWNER)).andExpect(status().isConflict());
  }

  @Test
  void guardedUnlockRejectsWrongReasonOwnerOrGeneration() throws Exception {
    createCleanup();
    mvc.perform(unlock(tableUUID, "wrong-owner")).andExpect(status().isConflict());
    mvc.perform(unlock("previous-generation", OWNER)).andExpect(status().isConflict());
    mvc.perform(
            auth(delete(PATH + "/lock/LEGACY"))
                .param("expectedTableUUID", tableUUID)
                .param("expectedLockOwner", OWNER))
        .andExpect(status().isConflict());
    mvc.perform(unlock(tableUUID, "__UNRECORDED__")).andExpect(status().isConflict());
    mvc.perform(unlock(tableUUID, OWNER)).andExpect(status().isNoContent());
  }

  @ParameterizedTest
  @CsvSource(
      value = {"NULL,owner", "uuid,NULL", "'',owner", "uuid,''", "' ',owner", "uuid,' '"},
      nullValues = "NULL")
  void absentOrBlankGuardsAreBadRequests(String generation, String owner) throws Exception {
    MockHttpServletRequestBuilder request = auth(delete(CLEANUP_PATH));
    if (generation != null) {
      request.param("expectedTableUUID", generation);
    }
    if (owner != null) {
      request.param("expectedLockOwner", owner);
    }
    mvc.perform(request).andExpect(status().isBadRequest());
  }

  @Test
  void malformedReasonAndMissingCleanupGenerationAreBadRequests() throws Exception {
    mvc.perform(
            auth(delete(PATH + "/lock/UNKNOWN"))
                .param("expectedTableUUID", tableUUID)
                .param("expectedLockOwner", OWNER))
        .andExpect(status().isBadRequest());
    mvc.perform(
            auth(post(PATH + "/lock"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\"}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void preOwnerCleanupHasExplicitGuardedRecovery() throws Exception {
    storeLock(LockState.builder().locked(true).reason(LockReason.TIER3_AUTO_CLEANUP).build());
    mvc.perform(auth(delete(PATH + "/lock"))).andExpect(status().isConflict());
    mvc.perform(unlock(tableUUID, OWNER)).andExpect(status().isConflict());
    mvc.perform(unlock("previous-generation", "__UNRECORDED__")).andExpect(status().isConflict());
    mvc.perform(unlock(tableUUID, "__UNRECORDED__")).andExpect(status().isNoContent());
    mvc.perform(unlock(tableUUID, "__UNRECORDED__")).andExpect(status().isNoContent());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stagedTableReplaceProtectsCleanupPolicies(boolean omitPolicies) throws Exception {
    createCleanup();
    TableDto current = repository.findById(KEY).get();
    CreateUpdateTableRequestBody request =
        buildCreateUpdateTableRequestBody(current)
            .toBuilder()
            .stageReplace(true)
            .policies(
                Policies.builder().lockState(LockState.builder().locked(true).build()).build())
            .build();
    mvc.perform(
            auth(post("/v1/databases/" + DB + "/tables"))
                .header(HTTP_HEADER_SYSTEM_ACTION, "true")
                .contentType(MediaType.APPLICATION_JSON)
                .content(request.toJson()))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("Cleanup lock state")));
    request =
        request.toBuilder().policies(omitPolicies ? null : Policies.builder().build()).build();
    mvc.perform(
            auth(post("/v1/databases/" + DB + "/tables"))
                .header(HTTP_HEADER_SYSTEM_ACTION, "true")
                .contentType(MediaType.APPLICATION_JSON)
                .content(request.toJson()))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.policies.lockState.lockOwner").value(OWNER))
        .andExpect(jsonPath("$.policies.lockState.tableUUID").value(tableUUID));
    org.junit.jupiter.api.Assertions.assertEquals(
        current.getPolicies(), repository.findById(KEY).get().getPolicies());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void snapshotUpdateAndReplaceCannotChangeCleanupPolicies(boolean replace) throws Exception {
    createCleanup();
    TableDto current = repository.findById(KEY).get();
    CreateUpdateTableRequestBody request =
        buildCreateUpdateTableRequestBody(current)
            .toBuilder()
            .replaceCommit(replace)
            .policies(
                Policies.builder().lockState(LockState.builder().locked(true).build()).build())
            .build();
    mvc.perform(
            auth(put(PATH + "/iceberg/v2/snapshots"))
                .header(HTTP_HEADER_SYSTEM_ACTION, "true")
                .contentType(MediaType.APPLICATION_JSON)
                .content(snapshots(request).toJson()))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("Cleanup lock state")));
    org.junit.jupiter.api.Assertions.assertEquals(
        current.getPolicies(), repository.findById(KEY).get().getPolicies());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void genericCreationCannotSmuggleCleanupPolicies(boolean snapshotWrite) throws Exception {
    Map<String, String> properties = new HashMap<>(GET_TABLE_RESPONSE_BODY.getTableProperties());
    properties.put("openhouse.databaseId", DB);
    properties.put("openhouse.tableId", TABLE + "_smuggled");
    CreateUpdateTableRequestBody request =
        buildCreateUpdateTableRequestBody(GET_TABLE_RESPONSE_BODY)
            .toBuilder()
            .tableId(TABLE + "_smuggled")
            .tableProperties(properties)
            .policies(
                Policies.builder()
                    .lockState(
                        LockState.builder()
                            .locked(true)
                            .reason(LockReason.TIER3_AUTO_CLEANUP)
                            .build())
                    .build())
            .build();
    MockHttpServletRequestBuilder write =
        snapshotWrite
            ? put(PATH + "_smuggled/iceberg/v2/snapshots").content(snapshots(request).toJson())
            : post("/v1/databases/" + DB + "/tables").content(request.toJson());
    mvc.perform(auth(write).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("Cleanup lock state")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ordinaryWritesCannotEraseOmittedCleanupMetadata(boolean snapshotWrite) throws Exception {
    storeLock(
        LockState.builder()
            .locked(false)
            .reason(LockReason.TIER3_AUTO_CLEANUP)
            .lockOwner(OWNER)
            .tableUUID(tableUUID)
            .build());
    TableDto current = repository.findById(KEY).get();
    CreateUpdateTableRequestBody request =
        buildCreateUpdateTableRequestBody(current).toBuilder().policies(null).build();
    MockHttpServletRequestBuilder write =
        snapshotWrite
            ? put(PATH + "/iceberg/v2/snapshots").content(snapshots(request).toJson())
            : put(PATH).content(request.toJson());
    mvc.perform(auth(write).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
    org.junit.jupiter.api.Assertions.assertEquals(
        current.getPolicies(), repository.findById(KEY).get().getPolicies());
  }

  private IcebergSnapshotsRequestBody snapshots(CreateUpdateTableRequestBody request) {
    return IcebergSnapshotsRequestBody.builder()
        .baseTableVersion(request.getBaseTableVersion())
        .createUpdateTableRequestBody(request)
        .jsonSnapshots(Collections.emptyList())
        .build();
  }

  private void createCleanup() throws Exception {
    mvc.perform(
            auth(post(PATH + "/lock"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\",\"expectedTableUUID\":\""
                        + tableUUID
                        + "\",\"lockOwner\":\"spoofed-owner\"}"))
        .andExpect(status().isCreated());
  }

  private void storeLock(LockState lock) {
    TableDto current = repository.findById(KEY).get();
    Policies policies =
        current.getPolicies() == null ? Policies.builder().build() : current.getPolicies();
    repository.save(
        current
            .toBuilder()
            .tableVersion(current.getTableLocation())
            .policies(policies.toBuilder().lockState(lock).build())
            .build());
  }

  private MockHttpServletRequestBuilder unlock(String generation, String owner) throws Exception {
    return auth(delete(CLEANUP_PATH))
        .param("expectedTableUUID", generation)
        .param("expectedLockOwner", owner);
  }

  private MockHttpServletRequestBuilder auth(MockHttpServletRequestBuilder request)
      throws Exception {
    return request.header("Authorization", "Bearer " + new DummySecurityJWT(OWNER).buildNoopJWT());
  }
}
