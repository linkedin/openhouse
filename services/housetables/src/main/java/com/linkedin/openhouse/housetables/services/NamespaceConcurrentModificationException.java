package com.linkedin.openhouse.housetables.services;

public class NamespaceConcurrentModificationException extends NamespaceMetadataException {
  public NamespaceConcurrentModificationException(String message) {
    super(message);
  }

  public NamespaceConcurrentModificationException(String message, Throwable cause) {
    super(message, cause);
  }
}
