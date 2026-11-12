package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataAlreadyExistsException extends NamespaceMetadataException {
  public NamespaceMetadataAlreadyExistsException(Throwable cause) {
    super(409, "Namespace already exists", cause);
  }
}
