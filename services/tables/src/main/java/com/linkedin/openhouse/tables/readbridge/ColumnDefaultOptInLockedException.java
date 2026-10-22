package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * A commit without the column-default policy bypass changed or removed a committed {@code
 * required-capability.column-defaults=true}. Nothing has been written.
 */
public class ColumnDefaultOptInLockedException extends CommitRejectedException {

  public ColumnDefaultOptInLockedException(String message) {
    super(message);
  }
}
