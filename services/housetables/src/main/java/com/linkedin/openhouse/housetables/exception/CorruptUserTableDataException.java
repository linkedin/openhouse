package com.linkedin.openhouse.housetables.exception;

/**
 * A read selected a row whose stored discriminator is corrupt. Distinguished from a plain {@link
 * UserTableReadException} so the advice can render the converter's column-and-value diagnostic
 * rather than a generic dependency message.
 */
public class CorruptUserTableDataException extends UserTableReadException {

  public CorruptUserTableDataException(String message) {
    super(message);
  }

  public CorruptUserTableDataException(String message, Throwable cause) {
    super(message, cause);
  }
}
