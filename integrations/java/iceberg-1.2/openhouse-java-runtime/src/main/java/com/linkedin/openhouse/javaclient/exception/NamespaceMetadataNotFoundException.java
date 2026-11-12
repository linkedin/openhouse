package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataNotFoundException extends NamespaceMetadataException {
  public NamespaceMetadataNotFoundException(Throwable cause) {
    super(404, "Namespace metadata does not exist", cause);
  }
}
