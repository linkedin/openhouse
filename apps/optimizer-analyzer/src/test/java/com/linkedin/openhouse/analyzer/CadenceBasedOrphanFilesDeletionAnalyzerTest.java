package com.linkedin.openhouse.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CadenceBasedOrphanFilesDeletionAnalyzerTest {

  private static final Duration SUCCESS_INTERVAL = Duration.ofHours(24);
  private static final Duration FAILURE_INTERVAL = Duration.ofHours(1);

  private CadenceBasedOrphanFilesDeletionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer =
        new CadenceBasedOrphanFilesDeletionAnalyzer(
            new CadencePolicy(SUCCESS_INTERVAL, FAILURE_INTERVAL));
  }

  // --- isEnabled ---

  @Test
  void isEnabled_returnsTrue_whenPropertySet() {
    assertThat(analyzer.isEnabled(tableWithProperty("true"))).isTrue();
  }

  @Test
  void isEnabled_returnsFalse_whenPropertyAbsent() {
    assertThat(analyzer.isEnabled(tableWithProperty(null))).isFalse();
  }

  @Test
  void isEnabled_returnsFalse_whenPropertyFalse() {
    assertThat(analyzer.isEnabled(tableWithProperty("false"))).isFalse();
  }

  @Test
  void isEnabled_returnsFalse_whenTablePropertiesEmpty() {
    Table table = Table.builder().tableUuid("uuid").build();
    assertThat(analyzer.isEnabled(table)).isFalse();
  }

  // --- shouldSchedule: no existing op ---

  @Test
  void shouldSchedule_noOp_noHistory_returnsTrue() {
    assertThat(
            analyzer.shouldSchedule(tableWithProperty("true"), Optional.empty(), Optional.empty()))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_successHistoryAfterCooldown_returnsTrue() {
    Instant longAgo = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.empty(),
                Optional.of(historyWithStatus("SUCCESS", longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_successHistoryBeforeCooldown_returnsFalse() {
    Instant recent = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.empty(),
                Optional.of(historyWithStatus("SUCCESS", recent))))
        .isFalse();
  }

  @Test
  void shouldSchedule_noOp_failedHistoryAfterRetry_returnsTrue() {
    Instant longAgo = Instant.now().minus(FAILURE_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.empty(),
                Optional.of(historyWithStatus("FAILED", longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_noOp_failedHistoryBeforeRetry_returnsFalse() {
    Instant recent = Instant.now().minus(FAILURE_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.empty(),
                Optional.of(historyWithStatus("FAILED", recent))))
        .isFalse();
  }

  // --- shouldSchedule: active op (non-CANCELED) → analyzer stays out ---

  @Test
  void shouldSchedule_pending_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.PENDING)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduling_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.SCHEDULING)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduled_returnsFalse_regardlessOfHistory() {
    Instant historyAt = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.SCHEDULED)),
                Optional.of(historyWithStatus("SUCCESS", historyAt))))
        .isFalse();
  }

  // --- shouldSchedule: CANCELED → cadence on history ---

  @Test
  void shouldSchedule_canceled_successHistoryAfterCooldown_returnsTrue() {
    Instant longAgo = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.CANCELED)),
                Optional.of(historyWithStatus("SUCCESS", longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_canceled_successHistoryBeforeCooldown_returnsFalse() {
    Instant recent = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.CANCELED)),
                Optional.of(historyWithStatus("SUCCESS", recent))))
        .isFalse();
  }

  @Test
  void shouldSchedule_canceled_noHistory_returnsTrue() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus(OperationStatus.CANCELED)),
                Optional.empty()))
        .isTrue();
  }

  // --- helpers ---

  private Table tableWithProperty(String value) {
    Map<String, String> props =
        value == null
            ? Collections.emptyMap()
            : Map.of(CadenceBasedOrphanFilesDeletionAnalyzer.OFD_ENABLED_PROPERTY, value);
    return Table.builder()
        .tableUuid("test-uuid")
        .databaseName("db1")
        .tableId("tbl1")
        .tableProperties(props)
        .build();
  }

  private TableOperation opWithStatus(OperationStatus status) {
    return TableOperation.builder().status(status).build();
  }

  private TableOperationHistoryRow historyWithStatus(String status, Instant completedAt) {
    return TableOperationHistoryRow.builder()
        .id("hist-id")
        .tableUuid("test-uuid")
        .operationType("ORPHAN_FILES_DELETION")
        .completedAt(completedAt)
        .status(status)
        .build();
  }
}
