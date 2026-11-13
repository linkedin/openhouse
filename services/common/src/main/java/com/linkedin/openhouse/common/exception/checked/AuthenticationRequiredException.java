package com.linkedin.openhouse.common.exception.checked;

public class AuthenticationRequiredException extends NamespaceException {
  public AuthenticationRequiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
