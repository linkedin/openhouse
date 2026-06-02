package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.List;

/**
 * A stateless bucketing strategy. Given a flat list of {@link BinItem}s, returns one {@link Bin}
 * per batch the scheduler should submit. Implementations do no IO and hold no mutable state.
 */
public interface BinPacker {
  OperationTypeDto getOperationType();

  List<Bin> pack(List<BinItem> items);
}
