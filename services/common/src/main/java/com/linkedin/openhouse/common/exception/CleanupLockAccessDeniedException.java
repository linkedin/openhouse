package com.linkedin.openhouse.common.exception;

/** An otherwise-authorized data operation was denied by an active cleanup lock. */
public class CleanupLockAccessDeniedException extends UnsupportedClientOperationException {
  public CleanupLockAccessDeniedException(String message) {
    super(Operation.LOCKED_TABLE_OPERATION, message);
  }
}
