package com.linkedin.openhouse.optimizer.scheduler.binpack;

/**
 * A schedulable unit produced by a {@link BinPacker}. Each bin owns the work for a single Spark job
 * — claiming the operations it covers, launching, and recording the outcome.
 */
public interface Bin {
  void schedule();
}
