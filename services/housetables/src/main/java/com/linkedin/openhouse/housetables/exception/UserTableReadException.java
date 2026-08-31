package com.linkedin.openhouse.housetables.exception;

/** A read failed for a reason other than absence, which is an empty {@link java.util.Optional}. */
public class UserTableReadException extends UserTablePersistenceException {

  public UserTableReadException(String message) {
    super(message);
  }

  public UserTableReadException(String message, Throwable cause) {
    super(message, cause);
  }
}
