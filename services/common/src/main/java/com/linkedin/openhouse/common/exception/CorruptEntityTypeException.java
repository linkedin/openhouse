package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating that a stored discriminator value is outside the vocabulary the column is
 * allowed to hold. This is server-side data corruption rather than a bad request, so the advice
 * maps it to a server error; it extends {@link IllegalArgumentException} so the callers already
 * catching one keep working.
 */
public class CorruptEntityTypeException extends IllegalArgumentException {

  public CorruptEntityTypeException(String message) {
    super(message);
  }

  public CorruptEntityTypeException(String message, Throwable cause) {
    super(message, cause);
  }
}
