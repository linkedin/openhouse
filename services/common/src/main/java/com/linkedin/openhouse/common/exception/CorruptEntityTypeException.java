package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating a stored discriminator value outside the vocabulary its column may hold.
 * Server-side corruption rather than a bad request, so the advice maps it to a server error; it
 * extends {@link IllegalArgumentException} so existing callers keep catching it.
 */
public class CorruptEntityTypeException extends IllegalArgumentException {

  public CorruptEntityTypeException(String message) {
    super(message);
  }

  public CorruptEntityTypeException(String message, Throwable cause) {
    super(message, cause);
  }
}
