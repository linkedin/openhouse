package com.linkedin.openhouse.housetables.exception;

/**
 * Root of the House Tables persistence failures this module owns. Unchecked because {@code
 * MetricsReporter.executeWithStats} declares no checked failure, and because Spring's default
 * {@code @Transactional} rollback triggers on unchecked failures only.
 */
public class UserTablePersistenceException extends RuntimeException {

  public UserTablePersistenceException(String message) {
    super(message);
  }

  public UserTablePersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
