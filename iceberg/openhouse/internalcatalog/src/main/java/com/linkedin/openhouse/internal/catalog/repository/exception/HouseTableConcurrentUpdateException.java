package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseTableConcurrentUpdateException extends HouseTableRepositoryException {

  public HouseTableConcurrentUpdateException(String message, Throwable cause) {
    super(message, cause);
  }
}
