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
  private static final Duration STALE_TIMEOUT = Duration.ofHours(3);

  private CadenceBasedStatsCollectionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer =
        new CadenceBasedStatsCollectionAnalyzer(
            new CadencePolicy(SUCCESS_INTERVAL, FAILURE_INTERVAL, STALE_TIMEOUT));
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
    assertThat(
            analyzer.shouldSchedule(tableWithProperty(true), Optional.empty(), Optional.empty(), 0))
        .isTrue();
  }

  @Test
  void shouldSchedule_successOlderThan24h_returnsTrue() {
    Instant longAgo = Instant.now().minus(SUCCESS_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, longAgo)),
                0))
        .isTrue();
  }

  @Test
  void shouldSchedule_successWithin24h_returnsFalse() {
    Instant recent = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, recent)),
                0))
        .isFalse();
  }

  @Test
  void shouldSchedule_failedAfterRetryInterval_returnsTrue() {
    Instant longAgo = Instant.now().minus(FAILURE_INTERVAL).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, longAgo)),
                1))
        .isTrue();
  }

  @Test
  void shouldSchedule_failedWithinRetryInterval_returnsFalse() {
    Instant recent = Instant.now().minus(FAILURE_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, recent)),
                1))
        .isFalse();
  }

  // --- circuit breaker: exponential backoff once the consecutive-failure streak hits threshold ---

  @Test
  void shouldSchedule_belowStreakThreshold_usesFlatFailureInterval_returnsTrue() {
    // 2 consecutive failures (< threshold 3): still the flat 1h retry, so a 90-min-old failure is
    // eligible again.
    Instant ninetyMinAgo = Instant.now().minus(Duration.ofMinutes(90));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, ninetyMinAgo)),
                2))
        .isTrue();
  }

  @Test
  void shouldSchedule_atStreakThreshold_backsOff_returnsFalseWithinBackoff() {
    // 3 consecutive failures: backoff = 1h * 2^(3-1) = 4h, so a 3h-old failure is NOT yet eligible.
    Instant threeHoursAgo = Instant.now().minus(Duration.ofHours(3));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, threeHoursAgo)),
                3))
        .isFalse();
  }

  @Test
  void shouldSchedule_atStreakThreshold_backsOff_returnsTrueAfterBackoff() {
    // Same 4h backoff; a 5h-old failure is past it and becomes eligible again.
    Instant fiveHoursAgo = Instant.now().minus(Duration.ofHours(5));
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.empty(),
                Optional.of(historyWithStatus(HistoryStatusDto.FAILED, fiveHoursAgo)),
                3))
        .isTrue();
  }

  // --- shouldSchedule: active op (pending / in-progress) → stay out ---

  @Test
  void shouldSchedule_pending_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.PENDING)),
                Optional.empty(),
                0))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduling_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.SCHEDULING)),
                Optional.empty(),
                0))
        .isFalse();
  }

  @Test
  void shouldSchedule_scheduled_returnsFalse() {
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opWithStatus(OperationStatusDto.SCHEDULED)),
                Optional.empty(),
                0))
        .isFalse();
  }

  // --- shouldSchedule: SCHEDULING/SCHEDULED past the stale timeout → dead job, reschedule ---

  @Test
  void shouldSchedule_scheduledPastStaleTimeout_noHistory_returnsTrue() {
    // Job submitted long ago but never reached a terminal state (missed/hung callback): the
    // SCHEDULED row is treated as dead so the table is rescheduled rather than wedged forever.
    Instant staleAt = Instant.now().minus(STALE_TIMEOUT).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opScheduledAt(OperationStatusDto.SCHEDULED, staleAt)),
                Optional.empty(),
                0))
        .isTrue();
  }

  @Test
  void shouldSchedule_scheduledWithinStaleTimeout_returnsFalse() {
    // A recently-submitted job is still live; the analyzer stays out.
    Instant recent = Instant.now().minus(STALE_TIMEOUT).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opScheduledAt(OperationStatusDto.SCHEDULED, recent)),
                Optional.empty(),
                0))
        .isFalse();
  }

  @Test
  void shouldSchedule_schedulingPastStaleTimeout_returnsTrue() {
    // A scheduler that claimed the row but died before recording a jobId also wedges without this.
    Instant staleAt = Instant.now().minus(STALE_TIMEOUT).minusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opScheduledAt(OperationStatusDto.SCHEDULING, staleAt)),
                Optional.empty(),
                0))
        .isTrue();
  }

  @Test
  void shouldSchedule_staleScheduled_stillHonorsHistoryCadence_returnsFalse() {
    // Even when the SCHEDULED row is dead, a recent successful collection means the stats are
    // fresh,
    // so the cadence guard still suppresses a redundant reschedule.
    Instant staleAt = Instant.now().minus(STALE_TIMEOUT).minusSeconds(60);
    Instant recentSuccess = Instant.now().minus(SUCCESS_INTERVAL).plusSeconds(60);
    assertThat(
            analyzer.shouldSchedule(
                tableWithProperty(true),
                Optional.of(opScheduledAt(OperationStatusDto.SCHEDULED, staleAt)),
                Optional.of(historyWithStatus(HistoryStatusDto.SUCCESS, recentSuccess)),
                0))
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

  private TableOperationDto opScheduledAt(OperationStatusDto status, Instant scheduledAt) {
    return TableOperationDto.builder().status(status).scheduledAt(scheduledAt).build();
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
