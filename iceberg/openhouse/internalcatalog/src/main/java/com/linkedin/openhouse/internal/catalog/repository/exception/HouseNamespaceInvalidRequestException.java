package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceInvalidRequestException extends HouseNamespaceException {
  public HouseNamespaceInvalidRequestException() {
    super("Invalid namespace request");
  }

  public HouseNamespaceInvalidRequestException(Throwable cause) {
    super("Invalid namespace request", cause);
  }
}
