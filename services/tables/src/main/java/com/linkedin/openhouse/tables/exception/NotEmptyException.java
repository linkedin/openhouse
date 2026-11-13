package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.common.exception.checked.NamespaceException;

public class NotEmptyException extends NamespaceException {
  public NotEmptyException(String message) {
    super(message);
  }

  public NotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
