package com.linkedin.openhouse.internal.catalog.repository.exception;

public class HouseNamespaceInvalidPropertiesException extends HouseNamespaceException {
  public HouseNamespaceInvalidPropertiesException(Throwable cause) {
    super("Invalid namespace property update", cause);
  }
}
