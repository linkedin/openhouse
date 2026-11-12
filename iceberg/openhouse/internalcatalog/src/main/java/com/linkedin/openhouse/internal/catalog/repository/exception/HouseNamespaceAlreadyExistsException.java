package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceAlreadyExistsException extends HouseNamespaceException {
  public HouseNamespaceAlreadyExistsException() {
    super("Namespace already exists");
  }

  public HouseNamespaceAlreadyExistsException(Throwable cause) {
    super("Namespace already exists", cause);
  }
}
