package com.linkedin.openhouse.housetables.services;

public class InvalidNamespaceRequestException extends NamespaceMetadataException {
  public InvalidNamespaceRequestException(String message) {
    super(message);
  }

  public InvalidNamespaceRequestException(String message, Throwable cause) {
    super(message, cause);
  }
}
