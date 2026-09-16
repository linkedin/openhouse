package com.linkedin.openhouse.common.exception;

/**
 * Exception indicating a table's metadata could not be read because a storage dependency (e.g.
 * HDFS) was transiently unavailable — a timeout, connection failure, NameNode standby, an Iceberg
 * {@code ServiceUnavailableException}, or another retriable I/O error.
 *
 * <p>This is a server/dependency condition, not table corruption, so it is surfaced as a {@code 503
 * Service Unavailable} and the caller should retry. See BDP-108628: we must not misrepresent a
 * transient I/O failure as {@link InvalidTableMetadataException}.
 */
public class StorageDependencyUnavailableException extends RuntimeException {

  public StorageDependencyUnavailableException(
      String databaseId, String tableId, String reason, Throwable cause) {
    super(
        String.format(
            "Table %s.%s could not be loaded due to a transient storage dependency failure: %s",
            databaseId, tableId, reason),
        cause);
  }
}
