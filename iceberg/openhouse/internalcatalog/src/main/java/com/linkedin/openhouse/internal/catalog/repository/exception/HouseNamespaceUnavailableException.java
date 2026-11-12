package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceUnavailableException extends HouseNamespaceException {
  public HouseNamespaceUnavailableException() {
    super("Namespace metadata service unavailable");
  }

  public HouseNamespaceUnavailableException(Throwable cause) {
    super("Namespace metadata service unavailable", cause);
  }
}
