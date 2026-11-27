package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.TemporaryTable;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Schedules the drop of a temporary table (see {@link TemporaryTable}) once its expiration time has
 * been reached. A table is selected when its temporary flag is exactly {@code true}, its expiration
 * time is valid, and the current time is at or past that time. The tables service still loads an
 * expired table normally; this analyzer and its deletion job are what remove it. Each deletion job
 * drops one table by name.
 *
 * <p>Expiry depends on time, not on commits, so this analyzer runs only in the time-based scan and
 * is not triggered by commits. A deletion job reports SUCCESS when it drops the table, and also
 * when it skips a table that is no longer temporary or not yet expired. Because the owner can set a
 * new expiration time after such a skip, a SUCCESS ends scheduling only when it completed at or
 * after the table's current expiration time; an earlier SUCCESS belongs to a previous expiration
 * and is ignored. A FAILED deletion is retried one hour later, and the backoff never grows past one
 * hour, so repeated failures keep retrying on every hourly run. A submitted deletion that has not
 * finished after the default stale time is rescheduled. Together these keep deletion within 24
 * hours of expiration when a job fails or hangs.
 *
 * <p>Registered as a bean only when {@code analyzer.expired-table-deletion.enabled=true}; it is off
 * by default.
 */
@Component
@ConditionalOnProperty(
    name = "analyzer.expired-table-deletion.enabled",
    havingValue = "true",
    matchIfMissing = false)
public class ExpiredTableDeletionAnalyzer implements OperationAnalyzer {

  private final CadencePolicy cadencePolicy =
      new CadencePolicy(
          Duration.ofHours(1),
          Duration.ofHours(1),
          CadencePolicy.DEFAULT_ACTIVE_OP_STALE_TIMEOUT,
          CadencePolicy.DEFAULT_FAILURE_STREAK_THRESHOLD,
          Duration.ofHours(1));

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.EXPIRED_TABLE_DELETION;
  }

  @Override
  public boolean isEnabled(TableDto table) {
    return TemporaryTable.expirationTime(table.getTableProperties()).isPresent();
  }

  @Override
  public boolean triggersOnCommit(TableDto table) {
    return false;
  }

  @Override
  public boolean isActiveOpTimedOut(TableOperationDto op) {
    return cadencePolicy.isSubmissionTimedOut(op);
  }

  @Override
  public Instant nextEligibleAt(Instant from, boolean succeeded, int attempts) {
    return cadencePolicy.nextEligibleAt(from, succeeded, attempts);
  }

  @Override
  public boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory,
      int consecutiveFailures) {
    return TemporaryTable.expirationTime(table.getTableProperties())
        .filter(expirationTime -> !Instant.now().isBefore(expirationTime))
        .map(
            expirationTime -> {
              Optional<TableOperationsHistoryDto> historyForCurrentExpiration =
                  latestHistory.filter(
                      entry ->
                          entry.getStatus() != HistoryStatusDto.SUCCESS
                              || !entry.getCompletedAt().isBefore(expirationTime));
              boolean alreadyDeleted =
                  historyForCurrentExpiration
                      .map(entry -> entry.getStatus() == HistoryStatusDto.SUCCESS)
                      .orElse(false);
              return !alreadyDeleted
                  && cadencePolicy.shouldSchedule(
                      currentOp, historyForCurrentExpiration, consecutiveFailures);
            })
        .orElse(false);
  }
}
