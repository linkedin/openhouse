package com.linkedin.openhouse.common.exception.checked;

public class PermissionDeniedException extends NamespaceException {
  public PermissionDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
