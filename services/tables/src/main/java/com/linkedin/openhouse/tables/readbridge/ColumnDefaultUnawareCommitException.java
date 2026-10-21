package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * Type 2: a snapshot commit to a table with column defaults did not come from a client that applies
 * them. Nothing has been written.
 */
public class ColumnDefaultUnawareCommitException extends CommitRejectedException {

  public ColumnDefaultUnawareCommitException(String message) {
    super(message);
  }
}
