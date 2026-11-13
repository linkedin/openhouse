package com.linkedin.openhouse.common.exception.checked;

public class PermissionDeniedException extends DomainException {
  public PermissionDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
