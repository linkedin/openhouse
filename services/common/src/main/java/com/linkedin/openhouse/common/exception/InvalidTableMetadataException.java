package com.linkedin.openhouse.common.exception;

/** Exception to indicate that a table's Iceberg metadata is invalid or corrupt. */
public class InvalidTableMetadataException extends RuntimeException {

  public InvalidTableMetadataException(
      String databaseId, String tableId, String reason, Throwable cause) {
    super(
        String.format("Table %s.%s has invalid metadata: %s", databaseId, tableId, reason), cause);
  }
}
