package com.linkedin.openhouse.housetables.exception;

/**
 * A stored {@code entity_type} value outside the vocabulary its column may hold. Server-state
 * corruption, whatever wrote it, so it is never a client error; deliberately unrelated to {@link
 * IllegalArgumentException} so it cannot fall into the shared advice's 400 branch.
 *
 * <p>Unchecked because the JPA {@link javax.persistence.AttributeConverter} SPI declares no checked
 * failure. {@code JpaUserTableReadRepository} is the boundary that converts it, and the wrappers
 * carrying it, into the module persistence hierarchy.
 */
public class CorruptEntityTypeConversionException extends RuntimeException {

  public CorruptEntityTypeConversionException(String message) {
    super(message);
  }

  public CorruptEntityTypeConversionException(String message, Throwable cause) {
    super(message, cause);
  }
}
