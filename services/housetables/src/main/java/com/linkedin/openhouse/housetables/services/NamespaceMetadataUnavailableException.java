package com.linkedin.openhouse.housetables.services;

public class NamespaceMetadataUnavailableException extends NamespaceMetadataException {
  public NamespaceMetadataUnavailableException(String message) {
    super(message);
  }

  public NamespaceMetadataUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
