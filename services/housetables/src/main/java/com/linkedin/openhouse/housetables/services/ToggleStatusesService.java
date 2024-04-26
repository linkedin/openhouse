package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatus;

public interface ToggleStatusesService {
  /**
   * Obtain the status of a {@link com.linkedin.openhouse.housetables.api.spec.model.UserTable}'s
   * feature.
   *
   * @param featureId identifier of the feature
   * @param databaseId identifier of the database
   * @param tableId identifier of the table
   * @return {@link ToggleStatus} of the requested entity.
   */
  ToggleStatus getTableToggleStatus(String featureId, String databaseId, String tableId);
}
