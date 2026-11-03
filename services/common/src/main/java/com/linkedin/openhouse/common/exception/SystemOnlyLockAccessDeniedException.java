package com.linkedin.openhouse.common.exception;

/** An otherwise-authorized data operation was denied by an active SYSTEM_ONLY lock. */
public class SystemOnlyLockAccessDeniedException extends UnsupportedClientOperationException {
  public SystemOnlyLockAccessDeniedException(String message) {
    super(Operation.LOCKED_TABLE_OPERATION, message);
  }
}
