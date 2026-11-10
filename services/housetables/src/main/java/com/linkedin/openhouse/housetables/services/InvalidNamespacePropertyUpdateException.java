package com.linkedin.openhouse.housetables.services;

public class InvalidNamespacePropertyUpdateException extends NamespaceMetadataException {
  public InvalidNamespacePropertyUpdateException(String message) {
    super(message);
  }

  public InvalidNamespacePropertyUpdateException(String message, Throwable cause) {
    super(message, cause);
  }
}
