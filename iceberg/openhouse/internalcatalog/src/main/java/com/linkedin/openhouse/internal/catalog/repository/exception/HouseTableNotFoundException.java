package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseTableNotFoundException extends HouseTableRepositoryException {
  public HouseTableNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
