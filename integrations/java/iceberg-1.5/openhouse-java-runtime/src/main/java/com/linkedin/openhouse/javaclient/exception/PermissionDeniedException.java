package com.linkedin.openhouse.javaclient.exception;

public class PermissionDeniedException extends CatalogOperationException {
  public PermissionDeniedException(Throwable cause) {
    super("Namespace access denied", cause);
  }
}
