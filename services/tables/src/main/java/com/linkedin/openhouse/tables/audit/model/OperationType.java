package com.linkedin.openhouse.tables.audit.model;

/** The type of specific table operation */
public enum OperationType {
  CREATE,
  READ,
  UPDATE,
  COMMIT,
  DELETE,
  GRANT,
  REVOKE,
  STAGED_CREATE,
  STAGED_COMMIT
}
