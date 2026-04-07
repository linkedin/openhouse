package com.linkedin.openhouse.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.analyzer.model.Table;
import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrphanFilesDeletionAnalyzerTest {

  private static final Duration SUCCESS_INTERVAL = Duration.ofHours(24);
  private static final Duration FAILURE_INTERVAL = Duration.ofHours(1);
  private static final Duration SCHEDULED_TIMEOUT = Duration.ofHours(6);

  private OrphanFilesDeletionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer =
        new OrphanFilesDeletionAnalyzer(
            new CadencePolicy(SUCCESS_INTERVAL, FAILURE_INTERVAL, SCHEDULED_TIMEOUT));
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

  // --- shouldSchedule: PENDING / SCHEDULING ---

  @Test
  void shouldSchedule_pending_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("PENDING", null)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduling_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULING", null)),
                Optional.empty()))
        .isFalse();
  }

  // --- shouldSchedule: SCHEDULED + history ---

  @Test
  void shouldSchedule_scheduledNoHistory_withinTimeout_returnsFalse() {
    Instant recent = Instant.now().minus(SCHEDULED_TIMEOUT).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", recent)),
                Optional.empty()))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduledNoHistory_pastTimeout_returnsTrue() {
    Instant longAgo = Instant.now().minus(SCHEDULED_TIMEOUT).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", longAgo)),
                Optional.empty()))
        .isTrue();
  }

  @Test
  void shouldSchedule_scheduledWithNullScheduledAt_noHistory_returnsTrue() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", null)),
                Optional.empty()))
        .isTrue();
  }

  @Test
  void shouldSchedule_scheduledWithSuccessHistory_afterCooldown_returnsTrue() {
    Instant scheduledAt = Instant.now().minusSeconds(3600);
    Instant historyAt = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", scheduledAt)),
                Optional.of(historyWithStatus("SUCCESS", historyAt))))
        .isTrue();
  }

  @Test
  void shouldSchedule_scheduledWithSuccessHistory_beforeCooldown_returnsFalse() {
    Instant scheduledAt = Instant.now().minusSeconds(3600);
    Instant historyAt = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", scheduledAt)),
                Optional.of(historyWithStatus("SUCCESS", historyAt))))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduledWithFailedHistory_afterRetry_returnsTrue() {
    Instant scheduledAt = Instant.now().minusSeconds(3600);
    Instant historyAt = Instant.now().minus(FAILURE_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", scheduledAt)),
                Optional.of(historyWithStatus("FAILED", historyAt))))
        .isTrue();
  }

  @Test
  void shouldSchedule_scheduledWithFailedHistory_beforeRetry_returnsFalse() {
    Instant scheduledAt = Instant.now().minusSeconds(3600);
    Instant historyAt = Instant.now().minus(FAILURE_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty("true"),
                Optional.of(opWithStatus("SCHEDULED", scheduledAt)),
                Optional.of(historyWithStatus("FAILED", historyAt))))
        .isFalse();
  }

  // --- helpers ---

  private Table tableWithProperty(String value) {
    Map<String, String> props =
        value == null
            ? Collections.emptyMap()
            : Map.of(OrphanFilesDeletionAnalyzer.OFD_ENABLED_PROPERTY, value);
    return Table.builder()
        .tableUuid("test-uuid")
        .databaseId("db1")
        .tableId("tbl1")
        .tableProperties(props)
        .build();
  }

  private TableOperation opWithStatus(String status, Instant scheduledAt) {
    TableOperation op = new TableOperation();
    op.setStatus(status);
    op.setScheduledAt(scheduledAt);
    return op;
  }

  private TableOperationHistoryRow historyWithStatus(String status, Instant submittedAt) {
    return TableOperationHistoryRow.builder()
        .id("hist-id")
        .tableUuid("test-uuid")
        .operationType("ORPHAN_FILES_DELETION")
        .submittedAt(submittedAt)
        .status(status)
        .build();
  }
}
