package com.linkedin.openhouse.javaclient.exception;

public class NamespaceMetadataResponseException extends NamespaceMetadataException {
  public NamespaceMetadataResponseException(String message) {
    super(0, message);
  }

  public NamespaceMetadataResponseException(int status, Throwable cause) {
    super(status, "Namespace response is unusable", cause);
  }
}
