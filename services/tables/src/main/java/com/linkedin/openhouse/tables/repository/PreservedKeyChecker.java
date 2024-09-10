package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.tables.model.TableDto;

/**
 * Defining keys that are preserved for OpenHouse to use. Those keys, if present in table
 * properties, are read-only.
 */
public interface PreservedKeyChecker {

  /** @deprecated Please consider using {@link #isKeyPreservedForTable(String, TableDto) instead} */
  @Deprecated
  boolean isKeyPreserved(String key);

  /**
   * Table-aware interface for OpenHouse-preserved keys among table properties. Default to
   * isKeyPreserved for backward compatibility.
   */
  default boolean isKeyPreservedForTable(String key, TableDto tableDto) {
    return isKeyPreserved(key);
  }

  String describePreservedSpace();
}
