package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.List;
import java.util.Optional;

/**
 * Per-operation-type orchestrator the scheduler dispatches to. The packer loads its PENDING work,
 * groups it into batches, and returns a {@link Bin} for each batch. The scheduler then asks each
 * bin to {@link Bin#schedule() schedule} itself.
 */
public interface BinPacker {
  OperationTypeDto getOperationType();

  List<Bin> prepare(Optional<String> databaseName, Optional<String> tableName);
}
