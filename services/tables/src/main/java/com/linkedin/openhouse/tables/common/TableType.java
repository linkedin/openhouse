package com.linkedin.openhouse.tables.common;

/**
 * Enum for table type: primary table is normal table, replica table is a read-only copy of a
 * primary table
 */
public enum TableType {
  PRIMARY_TABLE,
  REPLICA_TABLE
}
