package com.linkedin.openhouse.common.exception.checked;

public class AlreadyExistsException extends NamespaceException {
  public AlreadyExistsException(String message) {
    super(message);
  }

  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
