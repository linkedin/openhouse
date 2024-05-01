package com.linkedin.openhouse.datalayout.datamaintenance;

/**
 * Determines if a table needs to be compacted via the function 'evaluate'. The implementation of
 * the decision function is dependent on the policy.
 */
public interface CompactionPolicy {

  /** Compute whether a table should be compacted. */
  boolean compact();
}
