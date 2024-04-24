package com.linkedin.openhouse.datalayout.execution.datamaintenance;

// TODO: I can't see the diagram in the overview doc but this would be
// the strategy that you trigger once you've made a decision to compact.
public interface CompactionStrategy {
  public void execute();
}
