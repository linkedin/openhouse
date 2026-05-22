package com.linkedin.openhouse.optimizer.api.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exercises the {@code GlobalExceptionHandler} contract across all three controllers — every
 * non-2xx response carries an {@link com.linkedin.openhouse.optimizer.api.error.ApiError} body with
 * {@code code}, {@code message}, and {@code path}.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
class ControllerErrorHandlingTest {

  @Autowired MockMvc mockMvc;
  @Autowired TableOperationsRepository operationsRepository;

  // --- /operations/{id}/update ---

  @Test
  void updateOperation_notFound_returns404WithCode() throws Exception {
    String id = UUID.randomUUID().toString();
    String body = "{\"operationId\":\"" + id + "\",\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("OPERATION_NOT_FOUND"))
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString(id)))
        .andExpect(jsonPath("$.path").value("/v1/optimizer/operations/" + id + "/update"));
  }

  @Test
  void updateOperation_pathBodyMismatch_returns400() throws Exception {
    String pathId = UUID.randomUUID().toString();
    String bodyId = UUID.randomUUID().toString();
    String body = "{\"operationId\":\"" + bodyId + "\",\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + pathId + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("PATH_BODY_MISMATCH"));
  }

  @Test
  void updateOperation_missingStatus_returns400Validation() throws Exception {
    String id = UUID.randomUUID().toString();
    String body = "{\"operationId\":\"" + id + "\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("status")));
  }

  @Test
  void updateOperation_missingOperationId_returns400Validation() throws Exception {
    String pathId = UUID.randomUUID().toString();
    String body = "{\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + pathId + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(
            jsonPath("$.message").value(org.hamcrest.Matchers.containsString("operationId")));
  }

  @Test
  void updateOperation_malformedJson_returns400Malformed() throws Exception {
    String pathId = UUID.randomUUID().toString();
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + pathId + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content("not json"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("MALFORMED_REQUEST"));
  }

  // --- /operations/{id} ---

  @Test
  void getTableOperation_notFound_returns404WithCode() throws Exception {
    String id = UUID.randomUUID().toString();
    mockMvc
        .perform(get("/v1/optimizer/operations/" + id))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("OPERATION_NOT_FOUND"))
        .andExpect(jsonPath("$.path").value("/v1/optimizer/operations/" + id));
  }

  // --- /operations (list) ---

  @Test
  void listOperations_missingLimit_returns400Missing() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("MISSING_PARAMETER"))
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("limit")));
  }

  @Test
  void listOperations_badLimit_returns400TypeMismatch() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations").param("limit", "abc"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"))
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("limit")));
  }

  @Test
  void listOperations_badEnum_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations").param("status", "BOGUS").param("limit", "10"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));
  }

  // --- /stats/{tableUuid} ---

  @Test
  void getTableStats_notFound_returns404WithCode() throws Exception {
    String uuid = UUID.randomUUID().toString();
    mockMvc
        .perform(get("/v1/optimizer/stats/" + uuid))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("STATS_NOT_FOUND"));
  }

  // --- /stats (upsert) ---

  @Test
  void upsertStats_missingRequiredField_returns400Validation() throws Exception {
    String uuid = UUID.randomUUID().toString();
    String body = "{\"tableName\":\"tbl1\"}"; // databaseName missing
    mockMvc
        .perform(
            put("/v1/optimizer/stats/" + uuid)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
        .andExpect(
            jsonPath("$.message").value(org.hamcrest.Matchers.containsString("databaseName")));
  }

  // --- /stats/{tableUuid}/history ---

  @Test
  void getStatsHistory_badSince_returns400() throws Exception {
    String uuid = UUID.randomUUID().toString();
    mockMvc
        .perform(
            get("/v1/optimizer/stats/" + uuid + "/history")
                .param("since", "not-a-date")
                .param("limit", "10"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));
  }

  // --- happy path sanity ---

  @Test
  void updateOperation_happyPath_stillReturns201() throws Exception {
    String id = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .createdAt(java.time.Instant.now())
            .scheduledAt(java.time.Instant.now())
            .jobId("job-x")
            .build());
    String body = "{\"operationId\":\"" + id + "\",\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.status").value("SUCCESS"));
  }
}
