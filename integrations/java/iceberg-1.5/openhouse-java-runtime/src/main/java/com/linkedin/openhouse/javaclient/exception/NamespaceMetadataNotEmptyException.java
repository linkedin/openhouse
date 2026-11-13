package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataNotEmptyException extends NamespaceMetadataException {
  public NamespaceMetadataNotEmptyException(Throwable cause) {
    super(409, "Namespace contains tables or views", cause);
  }
}
