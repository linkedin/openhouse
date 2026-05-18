package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import lombok.Value;

/**
 * A pending operation paired with the stats the bin packer will use as its cost source. Built by
 * the scheduler at scheduling time and handed to the {@link BinPacker} as the unit of packing.
 *
 * <p>{@link #stats} may be {@code null} when the table has no stats row yet — the packer treats
 * that as zero cost rather than dropping the candidate.
 */
@Value
public class SchedulingCandidate {
  TableOperation operation;
  TableStats stats;
}
