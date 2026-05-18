package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import lombok.NonNull;
import lombok.Value;

/**
 * A pending operation paired with the stats the bin packer will use as its cost source. Built by
 * the scheduler at scheduling time and handed to the {@link BinPacker} as the unit of packing.
 *
 * <p>Both fields are non-null. The scheduler filters out operations whose tables have no stats row
 * before constructing candidates.
 */
@Value
public class SchedulingCandidate {
  @NonNull TableOperation operation;
  @NonNull TableStats stats;
}
