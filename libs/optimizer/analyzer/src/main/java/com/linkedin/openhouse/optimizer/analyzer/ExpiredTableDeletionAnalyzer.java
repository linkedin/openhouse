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
 * Schedules a drop of a temporary table (see {@link TemporaryTable}) once its expiration time has
 * passed. A deletion reports SUCCESS only when the table no longer exists, so a SUCCESS ends
 * scheduling for that table. A FAILED deletion is retried on the cadence policy's failure backoff.
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

  // The 24-hour success interval never takes effect, because shouldSchedule stops at a SUCCESS.
  private final CadencePolicy cadencePolicy =
      new CadencePolicy(Duration.ofHours(24), Duration.ofHours(1));

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.EXPIRED_TABLE_DELETION;
  }

  @Override
  public boolean isEnabled(TableDto table) {
    return TemporaryTable.expirationTime(table.getTableProperties()).isPresent();
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
    boolean alreadyDeleted =
        latestHistory.map(entry -> entry.getStatus() == HistoryStatusDto.SUCCESS).orElse(false);
    boolean expired =
        TemporaryTable.expirationTime(table.getTableProperties())
            .map(expirationTime -> !Instant.now().isBefore(expirationTime))
            .orElse(false);
    return !alreadyDeleted
        && expired
        && cadencePolicy.shouldSchedule(currentOp, latestHistory, consecutiveFailures);
  }
}
