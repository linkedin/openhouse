package com.linkedin.openhouse.internal.catalog.repository.exception;

/** Checked failures of the independent namespace metadata resource. */
public abstract class HouseNamespaceException extends Exception {
  protected HouseNamespaceException(String message) {
    super(message);
  }

  protected HouseNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
