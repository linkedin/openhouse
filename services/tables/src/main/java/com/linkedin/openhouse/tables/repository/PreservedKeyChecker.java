package com.linkedin.openhouse.tables.repository;

/**
 * Defining keys that are preserved for OpenHouse to use. Those keys, if present in table
 * properties, are read-only.
 */
public interface PreservedKeyChecker {

  boolean isKeyPreserved(String key);

  String describePreservedSpace();
}
