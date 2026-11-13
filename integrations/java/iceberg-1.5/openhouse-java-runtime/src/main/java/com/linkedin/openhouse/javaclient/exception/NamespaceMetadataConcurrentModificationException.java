package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataConcurrentModificationException extends NamespaceMetadataException {
  public NamespaceMetadataConcurrentModificationException(Throwable cause) {
    super(409, "Namespace metadata changed concurrently", cause);
  }
}
