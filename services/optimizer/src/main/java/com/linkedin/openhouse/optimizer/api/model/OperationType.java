package com.linkedin.openhouse.optimizer.api.model;

/** Maintenance operation types supported by the continuous optimizer. */
public enum OperationType {
  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION
}
