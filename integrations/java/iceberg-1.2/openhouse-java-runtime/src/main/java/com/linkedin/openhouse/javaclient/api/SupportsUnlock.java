package com.linkedin.openhouse.javaclient.api;

import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsUnlock {

  /**
   * Unlock an OH table without loading it.
   *
   * <p>The following SQL command: ALTER TABLE [db.table] UNLOCK [REASON [reason]]
   *
   * <p>can be converted into following parameters
   *
   * @param tableIdentifier identifier for the table, ex: db.table
   * @param reason lock reason, or null to remove only a legacy lock, ex: SYSTEM_ONLY
   */
  void unlockTable(TableIdentifier tableIdentifier, String reason);
}
