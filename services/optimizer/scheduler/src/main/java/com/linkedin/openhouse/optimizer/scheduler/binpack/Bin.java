package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * One scheduling unit: the operation type the bin will run as, and the items the scheduler will
 * claim, narrow to claimed, and launch a single Spark job for. Pure data — the scheduler reads from
 * a bin to do the work; the bin does no IO itself.
 */
@AllArgsConstructor
@Getter
@ToString
public class Bin {
  private final OperationTypeDto operationType;
  private final List<BinItem> items;
}
