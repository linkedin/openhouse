package com.linkedin.openhouse.common.exception;

/**
 * A commit broke a rule the internal catalog checks before writing anything, such as a {@code
 * CommitPolicy}. Nothing has been written. Mapped to 400.
 *
 * <p>Abstract so every rejection names its rule: each rule throws its own subclass, and callers
 * that handle every rejection catch this type.
 *
 * <p>It is not a {@code CommitFailedException}: Iceberg retries that, and a rejected commit fails
 * the same way every time.
 */
public abstract class CommitRejectedException extends RuntimeException {

  protected CommitRejectedException(String message) {
    super(message);
  }

  protected CommitRejectedException(String message, Throwable cause) {
    super(message, cause);
  }
}
