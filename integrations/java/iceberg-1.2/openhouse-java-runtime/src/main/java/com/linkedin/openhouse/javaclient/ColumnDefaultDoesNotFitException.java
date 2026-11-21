package com.linkedin.openhouse.javaclient;

/**
 * A column default that does not fit its column's type in a schema version. Its message is the one
 * the user sees: it names the table and the column, in the form the server uses for the same
 * problem.
 */
class ColumnDefaultDoesNotFitException extends ReadBridgeException {

  ColumnDefaultDoesNotFitException(String message, Throwable cause) {
    super(Kind.CANNOT_BIND, message, cause);
  }
}
