package com.linkedin.openhouse.common.exception;

/**
 * The server could not produce the per-table {@code config}, such as read-bridge settings, because
 * the table's stored settings cannot be applied. Neither the request nor a dependency is at fault,
 * and retrying will not help, so the advice maps it to a server error.
 *
 * <p>Deliberately not an {@link IllegalArgumentException}: that would land it on the advice's 400
 * branch, and the Java client reads a 400 on refresh as a missing table.
 */
public class TableConfigUnavailableException extends RuntimeException {

  public TableConfigUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
