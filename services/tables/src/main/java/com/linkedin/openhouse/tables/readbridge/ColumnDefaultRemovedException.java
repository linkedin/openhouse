package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * Type 1: a commit to an opted-in table omitted the column default of a column the table still has.
 * Nothing has been written.
 */
public class ColumnDefaultRemovedException extends CommitRejectedException {

  public ColumnDefaultRemovedException(String message) {
    super(message);
  }
}
