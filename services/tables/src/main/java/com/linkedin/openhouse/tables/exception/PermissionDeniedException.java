package com.linkedin.openhouse.tables.exception;

public class PermissionDeniedException extends Exception {
  public PermissionDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
