package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Analyzer for the {@link OperationTypeDto#ORPHAN_FILES_DELETION} operation type. */
@Component
public class CadenceBasedOrphanFilesDeletionAnalyzer implements OperationAnalyzer {

  static final String OFD_ENABLED_PROPERTY = "maintenance.optimizer.ofd.enabled";

  private final CadencePolicy cadencePolicy;

  @Autowired
  public CadenceBasedOrphanFilesDeletionAnalyzer(
      @Value("${ofd.success-retry-hours:24}") long successRetryHours,
      @Value("${ofd.failure-retry-hours:1}") long failureRetryHours) {
    this.cadencePolicy =
        new CadencePolicy(Duration.ofHours(successRetryHours), Duration.ofHours(failureRetryHours));
  }

  /** Package-private for tests that supply a pre-built {@link CadencePolicy}. */
  CadenceBasedOrphanFilesDeletionAnalyzer(CadencePolicy cadencePolicy) {
    this.cadencePolicy = cadencePolicy;
  }

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.ORPHAN_FILES_DELETION;
  }

  @Override
  public boolean isEnabled(TableDto table) {
    return "true".equals(table.getTableProperties().get(OFD_ENABLED_PROPERTY));
  }

  @Override
  public boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory) {
    return cadencePolicy.shouldSchedule(currentOp, latestHistory);
  }
}
