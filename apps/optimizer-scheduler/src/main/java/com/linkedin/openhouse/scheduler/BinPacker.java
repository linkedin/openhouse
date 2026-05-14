package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.TableOperation;
import java.util.List;

/**
 * Strategy for packing a set of operations into bins for batched job submission. Implementations
 * encode the constraints of a particular packing dimension (file count, partition count, etc.);
 * binding to an operation type is the responsibility of the scheduler configuration, not the
 * strategy class.
 */
public interface BinPacker {

  /**
   * Pack {@code pending} into one or more bins. Each returned bin is a non-empty list of
   * operations. The relative order of operations within a bin is implementation-defined; the
   * returned list of bins is what the scheduler dispatches one job per.
   */
  List<List<TableOperation>> pack(List<TableOperation> pending);
}
