package com.linkedin.openhouse.javaclient.api;

import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsUnlock {

  /**
   * Removes a table's lock without loading it, for ALTER TABLE t UNLOCK [REASON r].
   *
   * @param tableIdentifier e.g. db.table
   * @param reason e.g. SYSTEM_ONLY; null removes only a legacy lock
   */
  void unlockTable(TableIdentifier tableIdentifier, String reason);
}
