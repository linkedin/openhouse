package com.linkedin.openhouse.javaclient.exception;

public class OperationFailedException extends CatalogOperationException {
  public OperationFailedException(String message) {
    super(message);
  }

  public OperationFailedException(Throwable cause) {
    super("Namespace operation failed", cause);
  }
}
