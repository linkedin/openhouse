package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.List;
import java.util.Map;

/**
 * Strategy for packing a set of operations into bins for batched job submission. Implementations
 * encode the constraints of a particular packing dimension (file count, partition count, etc.);
 * binding to an operation type is the responsibility of the scheduler configuration, not the
 * strategy class.
 *
 * <p>{@link TableStats} is the cost source at the interface boundary. Implementations project it
 * down to the minimal data needed to make their packing decision (e.g. file count for OFD) and do
 * not retain the full stats payload in the returned bins.
 */
public interface BinPacker {

  /**
   * Pack {@code pending} into one or more {@link Bin}s, using {@code statsByUuid} (keyed by {@link
   * TableOperation#getTableUuid()}) as the cost source. Each returned bin is non-empty; the
   * scheduler dispatches one Spark job per bin.
   */
  List<Bin> pack(List<TableOperation> pending, Map<String, TableStats> statsByUuid);
}
