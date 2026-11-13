package com.linkedin.openhouse.javaclient.exception;

/** Checked semantic failure of a catalog operation, independent of its transport. */
public abstract class CatalogOperationException extends Exception {
  protected CatalogOperationException(String message) {
    super(message);
  }

  protected CatalogOperationException(String message, Throwable cause) {
    super(message, cause);
  }
}
