package com.linkedin.openhouse.tables.exception;

import lombok.Getter;

@Getter
public class NamespaceException extends Exception {
  public enum Reason {
    INVALID_NAMESPACE,
    INVALID_PROPERTIES,
    NOT_EMPTY,
    ACCESS_DENIED,
    AUTHORIZATION_UNAVAILABLE
  }

  private final Reason reason;

  public NamespaceException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public NamespaceException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }
}
