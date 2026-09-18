package com.linkedin.openhouse.javaclient.api;

import com.linkedin.openhouse.javaclient.exception.TableLockException;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;

/** Manages persistent OpenHouse table locks. */
public interface SupportsTableLocking {

  void lockTable(TableIdentifier tableIdentifier, Optional<String> reason, Optional<String> message)
      throws TableLockException;

  void unlockTable(TableIdentifier tableIdentifier, Optional<String> reason)
      throws TableLockException;
}
