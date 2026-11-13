package com.linkedin.openhouse.common.exception.checked;

public class ServiceFailureException extends DomainException {
  public ServiceFailureException(String message) {
    super(message);
  }

  public ServiceFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
