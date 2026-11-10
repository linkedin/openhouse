package com.linkedin.openhouse.housetables.services;

public class NamespaceAlreadyExistsException extends NamespaceMetadataException {
  public NamespaceAlreadyExistsException(String message) {
    super(message);
  }

  public NamespaceAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
