package com.linkedin.openhouse.internal.catalog.repository.exception;

/** Exception thrown when contacting HTS timed out. */
public class HouseTableRepositoryTimeoutException extends HouseTableRepositoryException {
  public HouseTableRepositoryTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
