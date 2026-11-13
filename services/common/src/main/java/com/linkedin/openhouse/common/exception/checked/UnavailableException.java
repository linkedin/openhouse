package com.linkedin.openhouse.common.exception.checked;

public class UnavailableException extends NamespaceException {
  public UnavailableException(String message) {
    super(message);
  }

  public UnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
