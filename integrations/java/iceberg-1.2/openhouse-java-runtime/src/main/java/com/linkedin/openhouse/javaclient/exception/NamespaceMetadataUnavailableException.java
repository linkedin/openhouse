package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataUnavailableException extends NamespaceMetadataException {
  public NamespaceMetadataUnavailableException(int status, Throwable cause) {
    super(status, "Namespace metadata service unavailable", cause);
  }
}
