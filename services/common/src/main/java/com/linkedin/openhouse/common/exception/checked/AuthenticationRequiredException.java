package com.linkedin.openhouse.common.exception.checked;

public class AuthenticationRequiredException extends DomainException {
  public AuthenticationRequiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
