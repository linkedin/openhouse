package com.linkedin.openhouse.common.stats.model;

/**
 * Enum representing the type of operation performed during a dataset commit.
 *
 * <p>Defines the possible modification operations that can be applied to a dataset.
 */
public enum CommitOperation {
  /** Append new data to the dataset */
  APPEND,

  /** Overwrite existing data in the dataset */
  OVERWRITE,

  /** Delete data from the dataset */
  DELETE,

  /** Replace data in the dataset */
  REPLACE
}
