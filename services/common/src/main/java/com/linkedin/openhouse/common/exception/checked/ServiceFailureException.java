package com.linkedin.openhouse.common.exception.checked;

public class ServiceFailureException extends NamespaceException {
  public ServiceFailureException(String message) {
    super(message);
  }

  public ServiceFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
