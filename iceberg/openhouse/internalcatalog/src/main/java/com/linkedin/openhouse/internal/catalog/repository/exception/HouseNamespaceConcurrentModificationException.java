package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceConcurrentModificationException extends HouseNamespaceException {
  public HouseNamespaceConcurrentModificationException() {
    super("Namespace was modified concurrently");
  }

  public HouseNamespaceConcurrentModificationException(Throwable cause) {
    super("Namespace was modified concurrently", cause);
  }
}
