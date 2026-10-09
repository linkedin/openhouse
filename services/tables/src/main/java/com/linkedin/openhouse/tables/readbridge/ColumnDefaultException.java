package com.linkedin.openhouse.tables.readbridge;

/**
 * A table declares a column default that cannot be applied. {@link ColumnDefaultsSource} throws it
 * without knowing whose fault that is; {@link ReadBridgeConfigResolver} decides from which table it
 * was given: a request's table is a bad request, and a stored table is server state that cannot be
 * served.
 */
public class ColumnDefaultException extends Exception {

  public ColumnDefaultException(String message) {
    super(message);
  }

  public ColumnDefaultException(String message, Throwable cause) {
    super(message, cause);
  }
}
