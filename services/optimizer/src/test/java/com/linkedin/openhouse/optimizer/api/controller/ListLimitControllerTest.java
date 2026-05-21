package com.linkedin.openhouse.optimizer.api.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exercises the limit param across all four list endpoints: default applies when omitted,
 * out-of-range values are rejected with 400, and the {@code max-limit} bound is config-driven.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@TestPropertySource(
    properties = {
      "optimizer.api.list.default-limit=3",
      "optimizer.api.list.max-limit=50",
    })
@Transactional
class ListLimitControllerTest {

  @Autowired MockMvc mockMvc;
  @Autowired TableOperationsRepository operationsRepository;
  @Autowired TableOperationsHistoryRepository historyRepository;
  @Autowired TableStatsRepository statsRepository;
  @Autowired TableStatsHistoryRepository statsHistoryRepository;

  private String sharedTableUuid;

  @BeforeEach
  void seed() {
    sharedTableUuid = UUID.randomUUID().toString();
    for (int i = 0; i < 8; i++) {
      operationsRepository.save(
          TableOperationsRow.builder()
              .id(UUID.randomUUID().toString())
              .tableUuid(UUID.randomUUID().toString())
              .databaseName("db1")
              .tableName("tbl" + i)
              .operationType(OperationType.ORPHAN_FILES_DELETION)
              .status(OperationStatus.PENDING)
              .createdAt(Instant.now())
              .build());
      statsRepository.save(
          TableStatsRow.builder()
              .tableUuid(UUID.randomUUID().toString())
              .databaseName("db1")
              .tableName("tbl" + i)
              .updatedAt(Instant.now())
              .build());
      historyRepository.save(
          TableOperationsHistoryRow.builder()
              .id(UUID.randomUUID().toString())
              .tableUuid(sharedTableUuid)
              .databaseName("db1")
              .tableName("tbl")
              .operationType(OperationType.ORPHAN_FILES_DELETION)
              .status(com.linkedin.openhouse.optimizer.db.HistoryStatus.SUCCESS)
              .completedAt(Instant.now())
              .build());
      statsHistoryRepository.save(
          TableStatsHistoryRow.builder()
              .id(UUID.randomUUID().toString())
              .tableUuid(sharedTableUuid)
              .databaseName("db1")
              .tableName("tbl")
              .recordedAt(Instant.now())
              .build());
    }
  }

  // --- /operations ---

  @Test
  void listOperations_default_appliesConfiguredDefaultLimit() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(3));
  }

  @Test
  void listOperations_explicitLimit_honored() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations").param("limit", "5"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(5));
  }

  @Test
  void listOperations_zeroLimit_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations").param("limit", "0"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void listOperations_overMax_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations").param("limit", "51"))
        .andExpect(status().isBadRequest());
  }

  // --- /stats ---

  @Test
  void listStats_default_appliesConfiguredDefaultLimit() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/stats"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(3));
  }

  @Test
  void listStats_overMax_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/stats").param("limit", "51"))
        .andExpect(status().isBadRequest());
  }

  // --- /operations-history/{tableUuid} ---

  @Test
  void getOperationsHistory_default_appliesConfiguredDefaultLimit() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations-history/" + sharedTableUuid))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(3));
  }

  @Test
  void getOperationsHistory_overMax_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/operations-history/" + sharedTableUuid).param("limit", "51"))
        .andExpect(status().isBadRequest());
  }

  // --- /stats/{tableUuid}/history ---

  @Test
  void getStatsHistory_default_appliesConfiguredDefaultLimit() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/stats/" + sharedTableUuid + "/history"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(3));
  }

  @Test
  void getStatsHistory_overMax_returns400() throws Exception {
    mockMvc
        .perform(get("/v1/optimizer/stats/" + sharedTableUuid + "/history").param("limit", "51"))
        .andExpect(status().isBadRequest());
  }
}
