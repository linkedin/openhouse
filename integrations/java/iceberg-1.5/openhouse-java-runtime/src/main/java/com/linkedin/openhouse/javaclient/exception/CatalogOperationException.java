package com.linkedin.openhouse.javaclient.exception;

/** Checked catalog failure. A failed write may have changed the stored metadata. */
public class CatalogOperationException extends Exception {
  public CatalogOperationException(String message) {
    super(message);
  }

  public CatalogOperationException(String message, Throwable cause) {
    super(message, cause);
  }
}
