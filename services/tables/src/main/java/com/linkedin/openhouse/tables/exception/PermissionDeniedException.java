package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;

public class PermissionDeniedException extends NamespaceException {
  public PermissionDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
