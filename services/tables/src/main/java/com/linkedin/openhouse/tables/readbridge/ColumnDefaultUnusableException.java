package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * A commit declares a column default that cannot be applied. Nothing has been written. The cause is
 * the {@link ColumnDefaultException} from the {@link ColumnDefaultsSource}.
 */
public class ColumnDefaultUnusableException extends CommitRejectedException {

  public ColumnDefaultUnusableException(ColumnDefaultException cause) {
    super(cause.getMessage(), cause);
  }
}
