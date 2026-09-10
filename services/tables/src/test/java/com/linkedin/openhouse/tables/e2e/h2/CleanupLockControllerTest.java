package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.tables.model.TableModelConstants.GET_TABLE_RESPONSE_BODY;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.jayway.jsonpath.JsonPath;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(
    initializers = {
      PropertyOverrideContextInitializer.class,
      AuthorizationPropertiesInitializer.class
    })
public class CleanupLockControllerTest {
  @Autowired private MockMvc mvc;
  @Autowired private StorageManager storageManager;

  private String tablePath;
  private String tableUUID;

  @BeforeEach
  void createTable() throws Exception {
    String response =
        RequestAndValidateHelper.createTableAndValidateResponse(
                GET_TABLE_RESPONSE_BODY, mvc, storageManager)
            .getResponse()
            .getContentAsString();
    tableUUID = JsonPath.read(response, "$.tableUUID");
    tablePath =
        "/v1/databases/"
            + GET_TABLE_RESPONSE_BODY.getDatabaseId()
            + "/tables/"
            + GET_TABLE_RESPONSE_BODY.getTableId();
  }

  @AfterEach
  void deleteTable() throws Exception {
    RequestAndValidateHelper.deleteTableAndValidateResponse(mvc, GET_TABLE_RESPONSE_BODY);
  }

  @Test
  void cleanupLockMetadataSurvivesPersistenceAndMatchingRetry() throws Exception {
    createCleanupLock(tableUUID).andExpect(status().isCreated());
    String response =
        mvc.perform(MockMvcRequestBuilders.get(tablePath))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.policies.lockState.reason").value("TIER3_AUTO_CLEANUP"))
            .andExpect(jsonPath("$.policies.lockState.tableUUID").value(tableUUID))
            .andExpect(jsonPath("$.policies.lockState.lockOwner").isNotEmpty())
            .andReturn()
            .getResponse()
            .getContentAsString();
    Map<String, Object> originalLock = JsonPath.read(response, "$.policies.lockState");
    createCleanupLock(tableUUID).andExpect(status().isCreated());
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.policies.lockState").value(originalLock));

    String lockOwner = JsonPath.read(response, "$.policies.lockState.lockOwner");
    for (int i = 0; i < 2; i++) {
      mvc.perform(
              MockMvcRequestBuilders.delete(tablePath + "/lock/TIER3_AUTO_CLEANUP")
                  .param("expectedTableUUID", tableUUID)
                  .param("lockOwner", lockOwner))
          .andExpect(status().isNoContent());
    }
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.policies.lockState").isEmpty());
  }

  @Test
  void cleanupLockCannotReplaceLegacyLock() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(tablePath + "/lock")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"locked\":true,\"message\":\"legacy maintenance\"}"))
        .andExpect(status().isCreated());
    createCleanupLock(tableUUID).andExpect(status().isConflict());
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(jsonPath("$.policies.lockState.message").value("legacy maintenance"))
        .andExpect(jsonPath("$.policies.lockState.reason").isEmpty());
  }

  @Test
  void legacyRequestsCannotReplaceOrRemoveCleanupLock() throws Exception {
    createCleanupLock(tableUUID).andExpect(status().isCreated());
    mvc.perform(MockMvcRequestBuilders.delete(tablePath + "/lock"))
        .andExpect(status().isConflict());
    mvc.perform(
            MockMvcRequestBuilders.post(tablePath + "/lock")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"locked\":true,\"message\":\"legacy\"}"))
        .andExpect(status().isConflict());
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(jsonPath("$.policies.lockState.reason").value("TIER3_AUTO_CLEANUP"));
  }

  @Test
  void staleGenerationCannotCreateCleanupLock() throws Exception {
    createCleanupLock("previous-generation")
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.message", containsString("generation")));
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(jsonPath("$.policies.lockState").isEmpty());
  }

  @Test
  void staleLockRequestCannotAffectRecreatedTable() throws Exception {
    String previousUUID = tableUUID;
    deleteTable();
    createTable();
    createCleanupLock(previousUUID).andExpect(status().isConflict());
    createCleanupLock(tableUUID).andExpect(status().isCreated());
    String response =
        mvc.perform(MockMvcRequestBuilders.get(tablePath))
            .andReturn()
            .getResponse()
            .getContentAsString();
    String lockOwner = JsonPath.read(response, "$.policies.lockState.lockOwner");
    mvc.perform(
            MockMvcRequestBuilders.delete(tablePath + "/lock/TIER3_AUTO_CLEANUP")
                .param("expectedTableUUID", previousUUID)
                .param("lockOwner", lockOwner))
        .andExpect(status().isConflict());
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(jsonPath("$.policies.lockState.tableUUID").value(tableUUID));
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "owner", "generation", "reason"})
  void unlockRejectsMissingOrNonmatchingIdentity(String mismatch) throws Exception {
    createCleanupLock(tableUUID).andExpect(status().isCreated());
    String response =
        mvc.perform(MockMvcRequestBuilders.get(tablePath))
            .andReturn()
            .getResponse()
            .getContentAsString();
    String lockOwner = JsonPath.read(response, "$.policies.lockState.lockOwner");
    MockHttpServletRequestBuilder request =
        MockMvcRequestBuilders.delete(
            tablePath + "/lock/" + ("reason".equals(mismatch) ? "UNKNOWN" : "TIER3_AUTO_CLEANUP"));
    if (!"missing".equals(mismatch)) {
      request
          .param("expectedTableUUID", "generation".equals(mismatch) ? "stale" : tableUUID)
          .param("lockOwner", "owner".equals(mismatch) ? "another-owner" : lockOwner);
    }
    mvc.perform(request)
        .andExpect(
            "missing".equals(mismatch) || "reason".equals(mismatch)
                ? status().isBadRequest()
                : status().isConflict());
    mvc.perform(MockMvcRequestBuilders.get(tablePath))
        .andExpect(jsonPath("$.policies.lockState.reason").value("TIER3_AUTO_CLEANUP"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\"}",
        "{\"locked\":true,\"reason\":\"UNKNOWN\"}",
        "{\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\",\"expectedTableUUID\":\" \"}"
      })
  void rejectsInvalidCleanupCreation(String body) throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.post(tablePath + "/lock")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest());
  }

  private ResultActions createCleanupLock(String expectedTableUUID) throws Exception {
    return mvc.perform(
        MockMvcRequestBuilders.post(tablePath + "/lock")
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                "{\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\","
                    + "\"expectedTableUUID\":\""
                    + expectedTableUUID
                    + "\"}"));
  }
}
