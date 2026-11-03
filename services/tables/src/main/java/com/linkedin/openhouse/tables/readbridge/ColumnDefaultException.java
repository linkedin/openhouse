package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;

/**
 * A column default that cannot be applied or validated: a {@link ColumnDefaultsSource} cannot apply
 * a declared default, or {@link ReadBridgeStripProtection} rejects a write that drops one, commits
 * snapshots from a client that does not apply column defaults, or that it cannot read. Read bridge
 * does not decide what that means to its caller, except that {@link ReadBridgeConfigResolver}
 * reports a stored table's as {@link TableConfigUnavailableException}: its config cannot be
 * produced.
 */
public class ColumnDefaultException extends Exception {

  public ColumnDefaultException(String message) {
    super(message);
  }

  public ColumnDefaultException(String message, Throwable cause) {
    super(message, cause);
  }
}
