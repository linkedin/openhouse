package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.SoftDeletedUserTableKey;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;

/**
 * {@link SoftDeletedUserTableHtsApiHandler} is the API handler for soft deleted user tables. It
 * provides additional methods to restore soft deleted user tables and bulk delete them based on
 * purgeAfterMs.
 */
public interface SoftDeletedUserTableHtsApiHandler
    extends HouseTablesApiHandler<SoftDeletedUserTableKey, UserTable> {

  /**
   * Restores a soft deleted user table from an entity in a Soft Deleted User Tables to User Tables
   *
   * @param softDeletedUserTable table to restore
   * @return
   */
  ApiResponse<EntityResponseBody<UserTable>> restoreEntity(
      SoftDeletedUserTableKey softDeletedUserTable);

  /**
   * Deletes all soft deleted user tables that match the given tableId and databaseId, and are older
   * than purgeAfterMs
   *
   * @param databaseId
   * @param tableId
   * @param purgeAfterMs
   * @return
   */
  ApiResponse<Void> deleteEntities(String databaseId, String tableId, Long purgeAfterMs);
}
