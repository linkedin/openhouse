package com.linkedin.openhouse.jobs.exception;

/** Exception thrown when table validation fails */
public class TableValidationException extends RuntimeException {
  public TableValidationException(String message) {
    super(message);
  }

  public TableValidationException(String message, Throwable cause) {
    super(message, cause);
  };
}
