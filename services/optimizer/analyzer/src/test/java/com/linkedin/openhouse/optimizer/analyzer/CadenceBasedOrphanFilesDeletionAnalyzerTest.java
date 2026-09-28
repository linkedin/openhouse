package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CadenceBasedOrphanFilesDeletionAnalyzerTest {

  private static final Duration TEST_SUCCESS_INTERVAL = Duration.ofHours(24);
  private static final Duration TEST_FAILURE_INTERVAL = Duration.ofHours(1);
  private static final Duration TEST_MAX_IDLE = Duration.ofHours(168);

  private CadenceBasedOrphanFilesDeletionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer =
        new CadenceBasedOrphanFilesDeletionAnalyzer(
            new CadencePolicy(TEST_SUCCESS_INTERVAL, TEST_FAILURE_INTERVAL), TEST_MAX_IDLE);
  }

  // --- isEnabled ---

  @Test
  void isEnabled_returnsTrue_whenPropertySet() {
    assertThat(analyzer.isEnabled(tableWithProperty(true))).isTrue();
  }

  @Test
  void isEnabled_returnsFalse_whenPropertyFalse() {
    assertThat(analyzer.isEnabled(tableWithProperty(false))).isFalse();
  }

  @Test
  void isEnabled_returnsFalse_whenTablePropertiesEmpty() {
    TableDto table = TableDto.builder().tableUuid("uuid").build();
    assertThat(analyzer.isEnabled(table)).isFalse();
  }

  // --- shouldSchedule: no existing op ---

  @Test
  void shouldSchedule_noOp_noHistory_returnsTrue() {
    assertThat(analyzer.shouldSchedule(tableWithProperty(true), Optional.empty(), Optional.empty()))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_successHistoryAfterCooldown_committed_returnsTrue() {
    Instant longAgo = Instant.now().minus(TEST_SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                // committed after the last run => activity gate passes
                tableWithProperty(true, longAgo.plusSeconds(30)),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_successHistoryBeforeCooldown_returnsFalse() {
    Instant recent = Instant.now().minus(TEST_SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, recent))))
        .isFalse();
  }

  @Test
  void shouldSchedule_noOp_failedHistoryAfterRetry_committed_returnsTrue() {
    Instant longAgo = Instant.now().minus(TEST_FAILURE_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true, longAgo.plusSeconds(30)),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_failedHistoryBeforeRetry_returnsFalse() {
    Instant recent = Instant.now().minus(TEST_FAILURE_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, recent))))
        .isFalse();
  }

  // --- shouldSchedule: data-driven activity gate ---

  @Test
  void shouldSchedule_cadenceElapsed_butNoCommitsSinceLastRun_returnsFalse() {
    // Cadence elapsed (25h > 24h) but the table has not been written since the last run and is
    // still within the max-idle window => skip (this is the compute saving for idle tables).
    Instant lastRun = Instant.now().minus(Duration.ofHours(25));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true, lastRun.minusSeconds(60)), // last commit predates last run
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, lastRun))))
        .isFalse();
  }

  @Test
  void shouldSchedule_idleTable_missingUpdatedAt_withinMaxIdle_returnsFalse() {
    Instant lastRun = Instant.now().minus(Duration.ofHours(25));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true), // no updatedAt at all
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, lastRun))))
        .isFalse();
  }

  @Test
  void shouldSchedule_idleTable_pastMaxIdle_returnsTrue() {
    // No commits since last run, but idle longer than max-idle (169h > 168h) => safety-net sweep.
    Instant lastRun = Instant.now().minus(TEST_MAX_IDLE).minus(Duration.ofHours(1));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true, lastRun.minusSeconds(60)),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, lastRun))))
        .isTrue();
  }

  // --- shouldSchedule: active op (non-CANCELED) → analyzer stays out ---

  @Test
  void shouldSchedule_pending_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.PENDING)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduling_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.SCHEDULING)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduled_returnsFalse_regardlessOfHistory() {
    Instant historyAt = Instant.now().minus(TEST_SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.SCHEDULED)),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, historyAt))))
        .isFalse();
  }

  // --- shouldSchedule: CANCELED → cadence on history ---

  @Test
  void shouldSchedule_canceled_successHistoryAfterCooldown_committed_returnsTrue() {
    Instant longAgo = Instant.now().minus(TEST_SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true, longAgo.plusSeconds(30)),
                Optional.of(opWithStatus(OperationStatusDto.CANCELED)),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_canceled_successHistoryBeforeCooldown_returnsFalse() {
    Instant recent = Instant.now().minus(TEST_SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.CANCELED)),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, recent))))
        .isFalse();
  }

  @Test
  void shouldSchedule_canceled_noHistory_returnsTrue() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.CANCELED)),
                Optional.empty()))
        .isTrue();
  }

  // --- helpers ---

  private TableDto tableWithProperty(boolean enabled) {
    return tableWithProperty(enabled, null);
  }

  private TableDto tableWithProperty(boolean enabled, Instant updatedAt) {
    return TableDto.builder()
        .tableUuid("test-uuid")
        .databaseName("db1")
        .tableId("tbl1")
        .updatedAt(updatedAt)
        .tableProperties(
            Map.of(
                CadenceBasedOrphanFilesDeletionAnalyzer.OFD_ENABLED_PROPERTY,
                Boolean.toString(enabled)))
        .build();
  }

  private TableOperationDto opWithStatus(OperationStatusDto status) {
    return TableOperationDto.builder().status(status).build();
  }

  private TableOperationsHistoryDto historyWithStatus(
      HistoryStatusDto status, Instant completedAt) {
    return TableOperationsHistoryDto.builder()
        .id("hist-id")
        .tableUuid("test-uuid")
        .operationType(
            com.linkedin.openhouse.optimizer.model.OperationTypeDto.ORPHAN_FILES_DELETION)
        .completedAt(completedAt)
        .status(status)
        .build();
  }
}
