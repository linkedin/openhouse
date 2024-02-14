package com.linkedin.openhouse.internal.catalog.exception;

/**
 * The exception thrown when the load-snapshot API detects invalid snapshot provided by clients.
 * TODO: Fill in more information in this exception class.
 */
public class InvalidIcebergSnapshotException extends RuntimeException {

  public InvalidIcebergSnapshotException(String errorMsg) {
    super(errorMsg);
  }
}
