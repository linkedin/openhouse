package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataForbiddenException extends NamespaceMetadataException {
  public NamespaceMetadataForbiddenException(Throwable cause) {
    super(403, "Namespace access denied", cause);
  }
}
