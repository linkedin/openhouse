package com.linkedin.openhouse.javaclient.exception;

public class InvalidRequestException extends CatalogOperationException {
  public InvalidRequestException(String message) {
    super(message);
  }

  public InvalidRequestException(Throwable cause) {
    super("Invalid namespace request", cause);
  }
}
