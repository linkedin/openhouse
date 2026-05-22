package com.linkedin.openhouse.optimizer.api.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import java.time.Instant;
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
 * Exercises what the controllers own: server-side validation on {@code updateOperation} (status
 * required) and 404s on missing rows. Assertions are status-code-only: MockMvc does not trigger
 * Spring's error-dispatch to {@code BasicErrorController}, so the response body of a {@link
 * org.springframework.web.server.ResponseStatusException} is empty in tests even though it is
 * populated in production (with {@code server.error.include-message=always}). Framework-level 4xx
 * (missing query param, malformed JSON, etc.) is left to Spring's defaults and not asserted.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
class ControllerErrorHandlingTest {

  @Autowired MockMvc mockMvc;
  @Autowired TableOperationsRepository operationsRepository;

  @Test
  void updateOperation_notFound_returns404() throws Exception {
    String id = UUID.randomUUID().toString();
    String body = "{\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isNotFound());
  }

  @Test
  void updateOperation_missingStatus_returns400() throws Exception {
    String id = UUID.randomUUID().toString();
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void getTableOperation_notFound_returns404() throws Exception {
    String id = UUID.randomUUID().toString();
    mockMvc.perform(get("/v1/optimizer/operations/" + id)).andExpect(status().isNotFound());
  }

  @Test
  void getTableStats_notFound_returns404() throws Exception {
    String uuid = UUID.randomUUID().toString();
    mockMvc.perform(get("/v1/optimizer/stats/" + uuid)).andExpect(status().isNotFound());
  }

  @Test
  void updateOperation_happyPath_returns201() throws Exception {
    String id = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .scheduledAt(Instant.now())
            .jobId("job-x")
            .build());
    String body = "{\"status\":\"SUCCESS\"}";
    mockMvc
        .perform(
            post("/v1/optimizer/operations/" + id + "/update")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.status").value("SUCCESS"));
  }
}
