package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating an unexpected/uncategorized failure while loading a table's Iceberg
 * metadata, treated as an OpenHouse implementation defect and surfaced as a {@code 500}.
 *
 * <p>Do NOT use this for conditions with a known classification: transient I/O failures use {@link
 * StorageDependencyUnavailableException} (503), and confirmed permanent corruption (missing
 * metadata/manifest file, Iceberg invariant violation, or malformed metadata) uses {@link
 * UnprocessableEntityException} (422).
 */
public class InvalidTableMetadataException extends RuntimeException {

  public InvalidTableMetadataException(
      String databaseId, String tableId, String reason, Throwable cause) {
    super(
        String.format("Table %s.%s has invalid metadata: %s", databaseId, tableId, reason), cause);
  }
}
