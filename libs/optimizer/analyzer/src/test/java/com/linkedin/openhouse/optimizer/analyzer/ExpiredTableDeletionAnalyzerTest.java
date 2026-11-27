package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.TemporaryTable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExpiredTableDeletionAnalyzerTest {

  private final ExpiredTableDeletionAnalyzer analyzer = new ExpiredTableDeletionAnalyzer();

  @Test
  void isEnabled_returnsTrue_forTemporaryTable() {
    assertThat(analyzer.isEnabled(temporaryTable(Instant.now()))).isTrue();
  }

  @Test
  void isEnabled_returnsFalse_forOrdinaryTable() {
    assertThat(analyzer.isEnabled(TableDto.builder().tableUuid("test-uuid").build())).isFalse();
  }

  @Test
  void isEnabled_returnsFalse_whenFlagIsNotTrue() {
    TableDto table =
        TableDto.builder()
            .tableUuid("test-uuid")
            .tableProperties(
                Map.of(
                    TemporaryTable.TEMPORARY_PROPERTY,
                    "TRUE",
                    TemporaryTable.EXPIRATION_TIME_PROPERTY,
                    format(Instant.now().minus(Duration.ofHours(1)))))
            .build();
    assertThat(analyzer.isEnabled(table)).isFalse();
  }

  @Test
  void triggersOnCommit_returnsFalse() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofHours(1)));
    assertThat(analyzer.triggersOnCommit(table)).isFalse();
  }

  @Test
  void isActiveOpTimedOut_returnsTrue_afterDefaultStaleTime() {
    assertThat(analyzer.isActiveOpTimedOut(submittedAgo(Duration.ofHours(2)))).isFalse();
    assertThat(analyzer.isActiveOpTimedOut(submittedAgo(Duration.ofHours(4)))).isTrue();
  }

  @Test
  void shouldSchedule_returnsTrue_whenSubmittedDeletionIsStale() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofHours(5)));
    assertThat(
            analyzer.shouldSchedule(
                table, Optional.of(submittedAgo(Duration.ofHours(4))), Optional.empty(), 0))
        .isTrue();
  }

  @Test
  void nextEligibleAt_isOneHourOut_afterRepeatedFailures() {
    Instant failedAt = Instant.now();
    assertThat(analyzer.nextEligibleAt(failedAt, false, 10))
        .isEqualTo(failedAt.plus(Duration.ofHours(1)));
  }

  @Test
  void shouldSchedule_returnsTrue_oneHourAfterRepeatedFailures() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofDays(1)));
    Instant failedAt = Instant.now().minus(Duration.ofMinutes(61));
    assertThat(
            analyzer.shouldSchedule(
                table,
                Optional.empty(),
                Optional.of(history(HistoryStatusDto.FAILED, failedAt)),
                6))
        .isTrue();
  }

  @Test
  void shouldSchedule_returnsTrue_atExpirationTime() {
    // The stored time has whole seconds, so the current second is at or past it.
    TableDto table = temporaryTable(Instant.now().truncatedTo(ChronoUnit.SECONDS));
    assertThat(analyzer.shouldSchedule(table, Optional.empty(), Optional.empty(), 0)).isTrue();
  }

  @Test
  void shouldSchedule_returnsFalse_beforeExpirationTime() {
    TableDto table = temporaryTable(Instant.now().plus(Duration.ofHours(1)));
    assertThat(analyzer.shouldSchedule(table, Optional.empty(), Optional.empty(), 0)).isFalse();
  }

  @Test
  void shouldSchedule_returnsTrue_afterExpirationTime() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofMinutes(1)));
    assertThat(analyzer.shouldSchedule(table, Optional.empty(), Optional.empty(), 0)).isTrue();
  }

  @Test
  void shouldSchedule_returnsFalse_whileDeletionIsPending() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofMinutes(1)));
    TableOperationDto pendingOp =
        TableOperationDto.builder().status(OperationStatusDto.PENDING).build();
    assertThat(analyzer.shouldSchedule(table, Optional.of(pendingOp), Optional.empty(), 0))
        .isFalse();
  }

  @Test
  void shouldSchedule_returnsTrue_afterFailedDeletionBackoff() {
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofDays(1)));
    Instant failedAt = Instant.now().minus(Duration.ofHours(2));
    assertThat(
            analyzer.shouldSchedule(
                table,
                Optional.of(completedOp(failedAt.plus(Duration.ofHours(1)))),
                Optional.of(history(HistoryStatusDto.FAILED, failedAt)),
                1))
        .isTrue();
  }

  @Test
  void shouldSchedule_returnsFalse_afterSuccessfulDeletion() {
    // On SUCCESS the service sets the op row's next eligible time one success interval out. Once
    // that time passes, the SUCCESS still keeps the deleted table from being scheduled again.
    TableDto table = temporaryTable(Instant.now().minus(Duration.ofDays(2)));
    Instant deletedAt = Instant.now().minus(Duration.ofDays(1)).minus(Duration.ofHours(1));
    assertThat(
            analyzer.shouldSchedule(
                table,
                Optional.of(completedOp(deletedAt.plus(Duration.ofDays(1)))),
                Optional.of(history(HistoryStatusDto.SUCCESS, deletedAt)),
                0))
        .isFalse();
  }

  private TableDto temporaryTable(Instant expirationTime) {
    return TableDto.builder()
        .tableUuid("test-uuid")
        .databaseName("db1")
        .tableId("tbl1")
        .tableProperties(
            Map.of(
                TemporaryTable.TEMPORARY_PROPERTY,
                "true",
                TemporaryTable.EXPIRATION_TIME_PROPERTY,
                format(expirationTime)))
        .build();
  }

  private static String format(Instant time) {
    return DateTimeFormatter.ISO_INSTANT.format(time.truncatedTo(ChronoUnit.SECONDS));
  }

  private static TableOperationDto submittedAgo(Duration age) {
    return TableOperationDto.builder()
        .status(OperationStatusDto.SCHEDULED)
        .scheduledAt(Instant.now().minus(age))
        .build();
  }

  /** The op row as the service leaves it after a reported outcome. */
  private TableOperationDto completedOp(Instant nextEligibleAt) {
    return TableOperationDto.builder()
        .status(OperationStatusDto.SCHEDULED)
        .nextEligibleAt(nextEligibleAt)
        .build();
  }

  private TableOperationsHistoryDto history(HistoryStatusDto status, Instant completedAt) {
    return TableOperationsHistoryDto.builder()
        .tableUuid("test-uuid")
        .operationType(OperationTypeDto.EXPIRED_TABLE_DELETION)
        .completedAt(completedAt)
        .status(status)
        .build();
  }
}
