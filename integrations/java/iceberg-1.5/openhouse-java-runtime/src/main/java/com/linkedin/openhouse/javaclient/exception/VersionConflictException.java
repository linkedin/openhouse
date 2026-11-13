package com.linkedin.openhouse.javaclient.exception;

public class VersionConflictException extends CatalogOperationException {
  public VersionConflictException(Throwable cause) {
    super("Namespace metadata changed concurrently", cause);
  }
}
