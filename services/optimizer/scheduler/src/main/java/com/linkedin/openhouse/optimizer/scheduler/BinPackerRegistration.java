package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Registration tuple for one operation type. Bundles the bucketing strategy with the {@link
 * BinItem} prototype the scheduler uses to project pending operations and their stats into packable
 * items.
 *
 * <p>Spring bean assembled by {@link
 * com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig}; {@link SchedulerRunner}
 * injects all registrations and indexes them by {@link #getOperationType()}.
 */
@AllArgsConstructor
@Getter
public class BinPackerRegistration {
  private final OperationTypeDto operationType;
  private final BinPacker packer;
  private final BinItem prototype;
}
