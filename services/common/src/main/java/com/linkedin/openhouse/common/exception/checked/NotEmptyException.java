package com.linkedin.openhouse.common.exception.checked;

public class NotEmptyException extends DomainException {
  public NotEmptyException(String message) {
    super(message);
  }

  public NotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
