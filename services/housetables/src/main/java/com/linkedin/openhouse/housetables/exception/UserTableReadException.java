package com.linkedin.openhouse.housetables.exception;

/**
 * A read against the user table store failed for a reason other than absence. Absence is an empty
 * {@link java.util.Optional}; this is the dependency failure that must never be mistaken for it.
 */
public class UserTableReadException extends UserTablePersistenceException {

  public UserTableReadException(String message) {
    super(message);
  }

  public UserTableReadException(String message, Throwable cause) {
    super(message, cause);
  }
}
