package com.linkedin.openhouse.optimizer.api.model;

/**
 * Maintenance operation types supported by the continuous optimizer.
 *
 * <p>Only {@code ORPHAN_FILES_DELETION} is currently implemented. Additional types will be added as
 * they are built out.
 */
public enum OperationType {
  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION
}
