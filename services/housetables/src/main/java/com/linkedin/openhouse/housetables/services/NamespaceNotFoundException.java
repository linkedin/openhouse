package com.linkedin.openhouse.housetables.services;

public class NamespaceNotFoundException extends NamespaceMetadataException {
  public NamespaceNotFoundException(String message) {
    super(message);
  }

  public NamespaceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
