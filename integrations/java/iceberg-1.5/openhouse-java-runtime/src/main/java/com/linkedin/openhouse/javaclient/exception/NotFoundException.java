package com.linkedin.openhouse.javaclient.exception;

public class NotFoundException extends CatalogOperationException {
  public NotFoundException(Throwable cause) {
    super("Namespace metadata does not exist", cause);
  }
}
