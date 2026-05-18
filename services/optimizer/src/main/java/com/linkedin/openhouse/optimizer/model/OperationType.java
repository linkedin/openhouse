package com.linkedin.openhouse.optimizer.model;

/**
 * Internal enum for the operation types the analyzer and scheduler know about. Intentionally
 * separate from the wire-API and DB representations so the internal model can evolve its set of
 * supported operations without churning either boundary.
 */
public enum OperationType {

  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION,

  /** Expires old Iceberg snapshots past the table's retention policy. */
  SNAPSHOT_EXPIRATION;

  /** Convert to the DB-layer counterpart. */
  public com.linkedin.openhouse.optimizer.db.OperationType toDb() {
    return com.linkedin.openhouse.optimizer.db.OperationType.valueOf(name());
  }

  /** Build the internal-model enum from the DB-layer counterpart. */
  public static OperationType fromDb(com.linkedin.openhouse.optimizer.db.OperationType v) {
    return v == null ? null : OperationType.valueOf(v.name());
  }
}
