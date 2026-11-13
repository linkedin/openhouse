package com.linkedin.openhouse.common.exception.checked;

public class InvalidPropertyUpdateException extends NamespaceException {
  public InvalidPropertyUpdateException(String message) {
    super(message);
  }

  public InvalidPropertyUpdateException(String message, Throwable cause) {
    super(message, cause);
  }
}
