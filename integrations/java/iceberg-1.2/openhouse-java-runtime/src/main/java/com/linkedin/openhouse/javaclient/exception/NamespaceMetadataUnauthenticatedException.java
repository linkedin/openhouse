package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataUnauthenticatedException extends NamespaceMetadataException {
  public NamespaceMetadataUnauthenticatedException(Throwable cause) {
    super(401, "Namespace authentication required", cause);
  }
}
