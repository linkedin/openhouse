package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceResponseException extends HouseNamespaceException {
  public HouseNamespaceResponseException() {
    super("Namespace metadata response is unusable");
  }

  public HouseNamespaceResponseException(Throwable cause) {
    super("Namespace metadata response is unusable", cause);
  }
}
