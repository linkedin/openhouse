package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceNotFoundException extends HouseNamespaceException {
  public HouseNamespaceNotFoundException() {
    super("Namespace does not exist");
  }

  public HouseNamespaceNotFoundException(Throwable cause) {
    super("Namespace does not exist", cause);
  }
}
