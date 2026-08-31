package com.linkedin.openhouse.housetables.exception;

/**
 * A stored {@code entity_type} outside the vocabulary its column may hold: server-state corruption
 * whatever wrote it. Deliberately unrelated to {@link IllegalArgumentException}, so it cannot fall
 * into the shared advice's 400 branch, and unchecked because the {@code AttributeConverter} SPI
 * declares no checked failure.
 */
public class CorruptEntityTypeConversionException extends RuntimeException {

  public CorruptEntityTypeConversionException(String message) {
    super(message);
  }

  public CorruptEntityTypeConversionException(String message, Throwable cause) {
    super(message, cause);
  }
}
