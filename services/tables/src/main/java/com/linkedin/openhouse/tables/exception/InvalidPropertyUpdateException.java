package com.linkedin.openhouse.tables.exception;

public class InvalidPropertyUpdateException extends Exception {
  public InvalidPropertyUpdateException(String message) {
    super(message);
  }

  public InvalidPropertyUpdateException(String message, Throwable cause) {
    super(message, cause);
  }
}
