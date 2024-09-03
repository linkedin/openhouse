package com.linkedin.openhouse.internal.catalog.toggle;

/** Interface to check if a feature is toggled-on for a table */
public interface TableFeatureToggle {
  /**
   * Determine if given feature is activated for the table.
   *
   * @param databaseId databaseId
   * @param tableId tableId
   * @param featureId featureId
   * @return True if the feature is activated for the table.
   */
  boolean isFeatureActivated(String databaseId, String tableId, String featureId);
}
