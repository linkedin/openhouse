package com.linkedin.openhouse.tables.exception;

public class NotEmptyException extends Exception {
  public NotEmptyException(String message) {
    super(message);
  }

  public NotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
