package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating a stored discriminator value outside the vocabulary its column may hold.
 * Server-side corruption rather than a bad request, so the advice maps it to a server error; it is
 * unchecked because the attribute converter that raises it declares no checked exception.
 */
public class CorruptEntityTypeException extends RuntimeException {

  public CorruptEntityTypeException(String message) {
    super(message);
  }

  public CorruptEntityTypeException(String message, Throwable cause) {
    super(message, cause);
  }
}
