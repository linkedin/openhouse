package com.linkedin.openhouse.common.exception;

/**
 * A stored discriminator value outside the vocabulary its column may hold. Server-state corruption
 * whatever wrote it, so the advice maps it to a server error.
 *
 * <p>Deliberately not an {@link IllegalArgumentException}: that would land it on the advice's 400
 * branch and report corrupt data as a bad request.
 */
public class CorruptEntityTypeException extends RuntimeException {

  public CorruptEntityTypeException(String message) {
    super(message);
  }

  public CorruptEntityTypeException(String message, Throwable cause) {
    super(message, cause);
  }
}
