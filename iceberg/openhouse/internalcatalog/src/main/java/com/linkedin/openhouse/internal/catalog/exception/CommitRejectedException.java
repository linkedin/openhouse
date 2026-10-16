package com.linkedin.openhouse.internal.catalog.exception;

/**
 * A {@code CommitPolicy} rejected a commit. Nothing has been written; doCommit rethrows {@link
 * #getCause()}, the error the caller reports, unchanged.
 */
public class CommitRejectedException extends Exception {

  public CommitRejectedException(RuntimeException error) {
    super(error.getMessage(), error, false, false);
  }

  @Override
  public synchronized RuntimeException getCause() {
    return (RuntimeException) super.getCause();
  }
}
