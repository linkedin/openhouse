package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CadenceBasedStatsCollectionAnalyzerTest {

  // Mirrors the production defaults: 24h between successful collections; a failed run retries after
  // a shorter interval (1h) rather than waiting out the full success cadence.
  private static final Duration SUCCESS_INTERVAL = Duration.ofHours(24);
  private static final Duration FAILURE_INTERVAL = Duration.ofHours(1);

  private CadenceBasedStatsCollectionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer =
        new CadenceBasedStatsCollectionAnalyzer(
            new CadencePolicy(SUCCESS_INTERVAL, FAILURE_INTERVAL));
  }

  @Test
  void operationType_isTableStatsCollection() {
    assertThat(analyzer.getOperationType()).isEqualTo(OperationTypeDto.TABLE_STATS_COLLECTION);
  }

  // --- isEnabled (opt-in) ---

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
    assertThat(analyzer.isEnabled(TableDto.builder().tableUuid("uuid").build())).isFalse();
  }

  // --- shouldSchedule: no active op → cadence on history ---

  @Test
  void shouldSchedule_noOp_noHistory_returnsTrue() {
    assertThat(analyzer.shouldSchedule(tableWithProperty(true), Optional.empty(), Optional.empty()))
        .isTrue();
  }

  @Test
  void shouldSchedule_successOlderThan24h_returnsTrue() {
    Instant longAgo = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_successWithin24h_returnsFalse() {
    Instant recent = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, recent))))
        .isFalse();
  }

  @Test
  void shouldSchedule_failedAfterRetryInterval_returnsTrue() {
    Instant longAgo = Instant.now().minus(FAILURE_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, longAgo))))
        .isTrue();
  }

  @Test
  void shouldSchedule_failedWithinRetryInterval_returnsFalse() {
    Instant recent = Instant.now().minus(FAILURE_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, recent))))
        .isFalse();
  }

  // --- shouldSchedule: active op (pending / in-progress) → stay out ---

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
  void shouldSchedule_scheduled_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.SCHEDULED)),
                Optional.empty()))
        .isFalse();
  }

  // --- helpers ---

  private TableDto tableWithProperty(boolean enabled) {
    return TableDto.builder()
        .tableUuid("test-uuid")
        .databaseName("db1")
        .tableId("tbl1")
        .tableProperties(
            Map.of(
                CadenceBasedStatsCollectionAnalyzer.STATS_ENABLED_PROPERTY,
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
        .operationType(OperationTypeDto.TABLE_STATS_COLLECTION)
        .completedAt(completedAt)
        .status(status)
        .build();
  }
}
