package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

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
class SystemOnlyLockControllerTest {
  private static final String DB = GET_TABLE_RESPONSE_BODY.getDatabaseId();
  private static final String TABLE = "system_only_lock_lifecycle";
  private static final String OWNER = "lock-owner";
  private static final String PATH = "/v1/databases/" + DB + "/tables/" + TABLE;
  private static final String SYSTEM_ONLY_PATH = PATH + "/lock/SYSTEM_ONLY";
  private static final TableDtoPrimaryKey KEY =
      TableDtoPrimaryKey.builder().databaseId(DB).tableId(TABLE).build();

  @Autowired private MockMvc mvc;
  @Autowired private OpenHouseInternalRepository repository;
  @MockBean private AuthorizationUtils authorizationUtils;

  @BeforeEach
  void createTable() throws Exception {
    Map<String, String> properties = new HashMap<>(GET_TABLE_RESPONSE_BODY.getTableProperties());
    properties.put(CatalogConstants.RTAS_ENABLED_TABLE_PROP, "true");
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
        .andExpect(status().isCreated());
  }

  @AfterEach
  void deleteTable() {
    repository.deleteById(KEY);
  }

  @Test
  void systemOnlyCreationNeedsOnlyExistingLockFieldsAndReason() throws Exception {
    mvc.perform(
            auth(post(PATH + "/lock"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"locked\":true,\"reason\":\"SYSTEM_ONLY\"}"))
        .andExpect(status().isCreated());
  }

  @Test
  void systemOnlyUnlockNeedsOnlyReasonAndExistingAuthorization() throws Exception {
    createSystemOnly();
    mvc.perform(auth(delete(SYSTEM_ONLY_PATH))).andExpect(status().isNoContent());
    verify(authorizationUtils, times(2))
        .checkLockTablePrivilege(any(), eq(OWNER), eq(Privileges.LOCK_ADMIN));
  }

  @Test
  void statusIsMetadataOnlyAndDoesNotRequireLockAdmin() throws Exception {
    createSystemOnly();
    doThrow(new AccessDeniedException("no lock admin"))
        .when(authorizationUtils)
        .checkLockTablePrivilege(any(), any(), any());
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$", aMapWithSize(1)))
        .andExpect(jsonPath("$.tableUUID").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.lockState.locked").value(true))
        .andExpect(jsonPath("$.lockState.reason").value("SYSTEM_ONLY"))
        .andExpect(jsonPath("$.lockState.lockOwner").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.lockState.tableUUID").doesNotHaveJsonPath())
        .andExpect(jsonPath("$.tableLocation").doesNotExist())
        .andExpect(jsonPath("$.schema").doesNotExist());
    verify(authorizationUtils)
        .checkTablePrivilege(any(), eq(OWNER), eq(Privileges.GET_TABLE_METADATA));
  }

  @Test
  void inactiveStatusIsNullAndMissingTableIsNotFound() throws Exception {
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
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
        .andExpect(jsonPath("$.lockState.message").value("Default"));
  }

  @Test
  void statusAndUnlockKeepExistingAuthorization() throws Exception {
    createSystemOnly();
    doThrow(new AccessDeniedException("denied"))
        .when(authorizationUtils)
        .checkTablePrivilege(any(), eq(OWNER), eq(Privileges.GET_TABLE_METADATA));
    mvc.perform(auth(get(PATH + "/lock"))).andExpect(status().isForbidden());
    doThrow(new AccessDeniedException("denied"))
        .when(authorizationUtils)
        .checkLockTablePrivilege(any(), eq(OWNER), eq(Privileges.LOCK_ADMIN));
    mvc.perform(unlock()).andExpect(status().isForbidden());
  }

  @Test
  void reasonTargetedUnlockWorksForAnotherLockAdminAndRetriesAreNoOps() throws Exception {
    createSystemOnly();
    mvc.perform(auth(delete(PATH + "/lock"))).andExpect(status().isConflict());
    mvc.perform(
            delete(SYSTEM_ONLY_PATH)
                .header(
                    "Authorization",
                    "Bearer " + new DummySecurityJWT("another-admin").buildNoopJWT()))
        .andExpect(status().isNoContent());
    String location = repository.findById(KEY).get().getTableLocation();
    mvc.perform(unlock()).andExpect(status().isNoContent());
    assertEquals(location, repository.findById(KEY).get().getTableLocation());
  }

  @Test
  void reasonTargetedUnlockRejectsWrongReason() throws Exception {
    createSystemOnly();
    mvc.perform(auth(delete(PATH + "/lock/LEGACY"))).andExpect(status().isConflict());
    mvc.perform(unlock()).andExpect(status().isNoContent());
  }

  @Test
  void malformedReasonIsABadRequest() throws Exception {
    mvc.perform(auth(delete(PATH + "/lock/UNKNOWN"))).andExpect(status().isBadRequest());
  }

  @Test
  void systemOnlyReasonCanBeUnlockedWithoutAdditionalMetadata() throws Exception {
    storeLock(LockState.builder().locked(true).reason(LockReason.SYSTEM_ONLY).build());
    mvc.perform(auth(delete(PATH + "/lock"))).andExpect(status().isConflict());
    mvc.perform(unlock()).andExpect(status().isNoContent());
    mvc.perform(auth(get(PATH + "/lock")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.lockState").value(nullValue()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stagedTableReplaceProtectsSystemOnlyPolicies(boolean omitPolicies) throws Exception {
    createSystemOnly();
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
                .contentType(MediaType.APPLICATION_JSON)
                .content(request.toJson()))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("SYSTEM_ONLY lock state")));
    request =
        request.toBuilder().policies(omitPolicies ? null : Policies.builder().build()).build();
    mvc.perform(
            auth(post("/v1/databases/" + DB + "/tables"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(request.toJson()))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.policies.lockState.reason").value("SYSTEM_ONLY"))
        .andExpect(jsonPath("$.policies.lockState.locked").value(true));
    assertEquals(current.getPolicies(), repository.findById(KEY).get().getPolicies());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void snapshotUpdateAndReplaceCannotChangeSystemOnlyPolicies(boolean replace) throws Exception {
    createSystemOnly();
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
                .contentType(MediaType.APPLICATION_JSON)
                .content(snapshots(request).toJson()))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("SYSTEM_ONLY lock state")));
    assertEquals(current.getPolicies(), repository.findById(KEY).get().getPolicies());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void genericCreationCannotSmuggleSystemOnlyPolicies(boolean snapshotWrite) throws Exception {
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
                        LockState.builder().locked(true).reason(LockReason.SYSTEM_ONLY).build())
                    .build())
            .build();
    MockHttpServletRequestBuilder write =
        snapshotWrite
            ? put(PATH + "_smuggled/iceberg/v2/snapshots").content(snapshots(request).toJson())
            : post("/v1/databases/" + DB + "/tables").content(request.toJson());
    mvc.perform(auth(write).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message", containsString("SYSTEM_ONLY lock state")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void inactiveSystemOnlyUsesOrdinaryPolicyUpdates(boolean snapshotWrite) throws Exception {
    storeLock(LockState.builder().locked(false).reason(LockReason.SYSTEM_ONLY).build());
    TableDto current = repository.findById(KEY).get();
    CreateUpdateTableRequestBody request =
        buildCreateUpdateTableRequestBody(current).toBuilder().policies(null).build();
    MockHttpServletRequestBuilder write =
        snapshotWrite
            ? put(PATH + "/iceberg/v2/snapshots").content(snapshots(request).toJson())
            : put(PATH).content(request.toJson());
    mvc.perform(auth(write).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
    Policies policies = repository.findById(KEY).get().getPolicies();
    assertNull(policies == null ? null : policies.getLockState());
  }

  private IcebergSnapshotsRequestBody snapshots(CreateUpdateTableRequestBody request) {
    return IcebergSnapshotsRequestBody.builder()
        .baseTableVersion(request.getBaseTableVersion())
        .createUpdateTableRequestBody(request)
        .jsonSnapshots(Collections.emptyList())
        .build();
  }

  private void createSystemOnly() throws Exception {
    mvc.perform(
            auth(post(PATH + "/lock"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"locked\":true,\"reason\":\"SYSTEM_ONLY\"}"))
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

  private MockHttpServletRequestBuilder unlock() throws Exception {
    return auth(delete(SYSTEM_ONLY_PATH));
  }

  private MockHttpServletRequestBuilder auth(MockHttpServletRequestBuilder request)
      throws Exception {
    return request.header("Authorization", "Bearer " + new DummySecurityJWT(OWNER).buildNoopJWT());
  }
}
