package com.linkedin.openhouse.internal.catalog.exception;

/**
 * A commit broke a rule doCommit checks before writing anything, such as a {@code CommitPolicy}.
 * Nothing has been written; the service that called the catalog decides how to report it.
 *
 * <p>It is not a {@code CommitFailedException}: Iceberg retries that, and a rejected commit fails
 * the same way every time.
 */
public class CommitRejectedException extends RuntimeException {

  public CommitRejectedException(String message) {
    super(message);
  }
}
