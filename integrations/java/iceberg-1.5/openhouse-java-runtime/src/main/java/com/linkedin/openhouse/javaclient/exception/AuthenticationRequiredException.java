package com.linkedin.openhouse.javaclient.exception;

public class AuthenticationRequiredException extends CatalogOperationException {
  public AuthenticationRequiredException(Throwable cause) {
    super("Namespace authentication required", cause);
  }
}
