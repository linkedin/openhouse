package com.linkedin.openhouse.jobs.util;

public interface TableFilter {
  /**
   * Function to apply filtering condition on tableMetadata.
   *
   * @param tableMetadata
   */
  boolean apply(TableMetadata tableMetadata);
}
