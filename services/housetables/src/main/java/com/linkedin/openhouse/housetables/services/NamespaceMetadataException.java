package com.linkedin.openhouse.housetables.services;

import lombok.Getter;

@Getter
public class NamespaceMetadataException extends Exception {
  public enum Reason {
    NOT_FOUND,
    ALREADY_EXISTS,
    INVALID_REQUEST,
    INVALID_UPDATE,
    CONCURRENT_MODIFICATION,
    UNAVAILABLE,
    STORAGE_FAILURE
  }

  private final Reason reason;

  public NamespaceMetadataException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public NamespaceMetadataException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }
}
