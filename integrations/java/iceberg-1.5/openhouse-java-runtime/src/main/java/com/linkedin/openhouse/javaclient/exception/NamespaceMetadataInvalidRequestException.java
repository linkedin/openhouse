package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataInvalidRequestException extends NamespaceMetadataException {
  public NamespaceMetadataInvalidRequestException(String message) {
    super(400, message);
  }

  public NamespaceMetadataInvalidRequestException(int status, Throwable cause) {
    super(status, "Invalid namespace request", cause);
  }
}
