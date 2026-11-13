package com.linkedin.openhouse.common.exception.checked;

public class InvalidRequestException extends NamespaceException {
  public InvalidRequestException(String message) {
    super(message);
  }

  public InvalidRequestException(String message, Throwable cause) {
    super(message, cause);
  }
}
