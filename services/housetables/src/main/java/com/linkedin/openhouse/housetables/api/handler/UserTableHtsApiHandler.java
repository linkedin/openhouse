package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;

/**
 * Invocation of generic type {@link HouseTablesApiHandler} using {@link UserTable} as the entity
 * type.
 */
public interface UserTableHtsApiHandler extends HouseTablesApiHandler<UserTableKey, UserTable> {

  /**
   * Function to Delete a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to delete.
   * @return the row as part of response body that would be returned to the client.
   */
  ApiResponse<Void> deleteEntity(UserTableKey key, boolean isSoftDelete);
}
