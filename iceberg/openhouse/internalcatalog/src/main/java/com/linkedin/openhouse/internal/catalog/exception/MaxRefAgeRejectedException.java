package com.linkedin.openhouse.internal.catalog.exception;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * A commit set {@code history.expire.max-ref-age-ms} outside (0, 7 days]. Nothing has been written.
 */
public class MaxRefAgeRejectedException extends CommitRejectedException {

  public MaxRefAgeRejectedException(String message) {
    super(message);
  }
}
