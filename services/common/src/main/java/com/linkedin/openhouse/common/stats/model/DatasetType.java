package com.linkedin.openhouse.common.stats.model;

/**
 * Enum representing the type of dataset.
 *
 * <p>Used to distinguish between partitioned and non-partitioned tables in dataset events.
 */
public enum DatasetType {
  /** Dataset with partition columns */
  PARTITIONED,

  /** Dataset without partition columns */
  NON_PARTITIONED
}
