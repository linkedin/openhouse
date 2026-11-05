package com.linkedin.openhouse.internal.catalog.repository.exception;

import lombok.Getter;

/** Checked failures of the independent database metadata resource. */
@Getter
public class HouseDatabaseException extends Exception {
  public enum Reason {
    BAD_REQUEST,
    NOT_FOUND,
    CONFLICT,
    INVALID_PROPERTIES,
    UNAVAILABLE,
    FAILURE
  }

  private final Reason reason;

  public HouseDatabaseException(Reason reason) {
    super("Database metadata operation failed");
    this.reason = reason;
  }

  public HouseDatabaseException(Reason reason, Throwable cause) {
    super("Database metadata operation failed", cause);
    this.reason = reason;
  }
}
