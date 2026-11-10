package com.linkedin.openhouse.housetables.services;

public abstract class NamespaceMetadataException extends Exception {
  protected NamespaceMetadataException(String message) {
    super(message);
  }

  protected NamespaceMetadataException(String message, Throwable cause) {
    super(message, cause);
  }
}
