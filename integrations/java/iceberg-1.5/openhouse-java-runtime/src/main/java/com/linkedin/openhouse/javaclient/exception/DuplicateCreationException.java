package com.linkedin.openhouse.javaclient.exception;

public class DuplicateCreationException extends CatalogOperationException {
  public DuplicateCreationException(Throwable cause) {
    super("Namespace already exists", cause);
  }
}
