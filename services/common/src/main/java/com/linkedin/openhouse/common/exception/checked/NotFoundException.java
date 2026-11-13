package com.linkedin.openhouse.common.exception.checked;

public class NotFoundException extends NamespaceException {
  public NotFoundException(String message) {
    super(message);
  }

  public NotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
