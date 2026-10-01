package com.linkedin.openhouse.javaclient.api;

import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsUnlock {

  /**
   * Remove the lock on an OH table without loading it.
   *
   * <p>The following SQL command: ALTER TABLE [db.table] UNLOCK [REASON reason]
   *
   * <p>can be converted into following parameters
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @param reason ex: SYSTEM_ONLY; null removes only a legacy lock; blank is rejected
   */
  void unlockTable(TableIdentifier tableIdentifier, String reason);
}
