package com.linkedin.openhouse.internal.catalog.repository.exception;

import lombok.Getter;

/** Checked failures of the independent database metadata resource. */
@Getter
public class HouseNamespaceException extends Exception {
  public enum Reason {
    BAD_REQUEST,
    NOT_FOUND,
    ALREADY_EXISTS,
    CONCURRENT_MODIFICATION,
    INVALID_PROPERTIES,
    UNAVAILABLE,
    UNKNOWN_OUTCOME,
    FAILURE
  }

  private final Reason reason;

  public HouseNamespaceException(Reason reason) {
    super("Database metadata operation failed");
    this.reason = reason;
  }

  public HouseNamespaceException(Reason reason, Throwable cause) {
    super("Database metadata operation failed", cause);
    this.reason = reason;
  }
}
