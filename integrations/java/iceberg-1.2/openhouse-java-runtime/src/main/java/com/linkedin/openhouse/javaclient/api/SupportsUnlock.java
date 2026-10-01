package com.linkedin.openhouse.javaclient.api;

import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsUnlock {

  /**
   * Remove the active lock on an OH table without loading the table.
   *
   * <p>The following SQL command: ALTER TABLE [db.table] UNLOCK [REASON reason]
   *
   * <p>can be converted into following parameters
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @param reason expected lock reason, ex: SYSTEM_ONLY; null removes only a legacy lock
   */
  void unlockTable(TableIdentifier tableIdentifier, String reason);
}
