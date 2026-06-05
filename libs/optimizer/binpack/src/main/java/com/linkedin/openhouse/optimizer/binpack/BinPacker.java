package com.linkedin.openhouse.optimizer.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.List;
import java.util.Map;

/**
 * Per-operation-type strategy. Given a flat list of operations and the corresponding stats, returns
 * one grouping per batch the scheduler should submit. The scheduler wraps each grouping into a
 * {@link Bin} with the registered operation type. Implementations do no IO and hold no mutable
 * state; the projection from {@code (op, stats)} to {@link BinItem} and the bucketing strategy both
 * live in the implementation.
 */
public interface BinPacker {
  List<List<BinItem>> pack(
      List<TableOperationDto> operations, Map<String, TableStatsDto> statsByTableUuid);
}
