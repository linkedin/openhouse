package com.linkedin.openhouse.javaclient.exception;

public class UnavailableException extends CatalogOperationException {
  public UnavailableException(Throwable cause) {
    super("Namespace metadata service unavailable", cause);
  }
}
