package com.linkedin.openhouse.internal.catalog.repository.exception;

/** Exception thrown when HTS returns a 5xx. */
public class HouseTableRepositoryStateUnkownException extends HouseTableRepositoryException {
  public HouseTableRepositoryStateUnkownException(String message, Throwable cause) {
    super(message, cause);
  }
}
