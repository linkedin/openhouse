package com.linkedin.openhouse.javaclient.exception;

public class NotEmptyException extends CatalogOperationException {
  public NotEmptyException(Throwable cause) {
    super("Namespace contains tables or views", cause);
  }
}
