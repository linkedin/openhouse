package com.linkedin.openhouse.common.exception;

/** A lock request that conflicts with the table's active lock. */
public class LockConflictException extends RuntimeException {
  public LockConflictException(String message) {
    super(message);
  }
}
