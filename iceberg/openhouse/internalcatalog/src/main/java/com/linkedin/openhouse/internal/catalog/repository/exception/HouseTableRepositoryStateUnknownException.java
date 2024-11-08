package com.linkedin.openhouse.internal.catalog.repository.exception;

/** Exception thrown when HTS returns a 5xx. */
public class HouseTableRepositoryStateUnknownException extends HouseTableRepositoryException {
  public HouseTableRepositoryStateUnknownException(String message, Throwable cause) {
    super(message, cause);
  }
}
