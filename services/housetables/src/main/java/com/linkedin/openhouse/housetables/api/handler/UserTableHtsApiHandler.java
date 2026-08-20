package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.EntityType;

/**
 * Invocation of generic type {@link HouseTablesApiHandler} using {@link UserTable} as the entity
 * type.
 *
 * <p>Type is selected by which method is called, never by an argument. The one exception is {@link
 * #renameEntity}, whose {@link EntityType} is bound internally by the controller from the route it
 * serves; no caller supplies it.
 */
public interface UserTableHtsApiHandler extends HouseTablesApiHandler<UserTableKey, UserTable> {

  /**
   * Function to Delete a row in a House Table given the key of the row.
   *
   * @param key The key object to identify the row to delete.
   * @return the row as part of response body that would be returned to the client.
   */
  ApiResponse<Void> deleteEntity(UserTableKey key, boolean isSoftDelete);

  /**
   * Reads whatever occupies the key, of any type, reporting the type it found. This is the
   * occupancy read; it is not a polymorphic catalog lookup.
   */
  ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey key);

  /** The view mirror of {@link #getEntity}. */
  ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey key);

  /** The view mirror of {@link #getEntities(UserTable)}. */
  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable entity);

  /** The view mirror of {@link #getEntities(UserTable, int, int, String)}. */
  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable entity, int page, int size, String sortBy);

  /** The view mirror of {@link #putEntity}. The controller stamps the type before dispatch. */
  ApiResponse<EntityResponseBody<UserTable>> putView(UserTable entity);

  /** Hard-deletes a view. There is no soft-delete flag: views have no soft-deleted store. */
  ApiResponse<Void> deleteView(UserTableKey key);

  /**
   * Renames a row of the given type. The type is bound by the controller from the route, so a
   * rename scoped to tables can never move a view.
   */
  ApiResponse<Void> renameEntity(UserTable fromEntity, UserTable toEntity, EntityType entityType);

  /**
   * Sealed in favour of the typed overload above. The shared {@link HouseTablesApiHandler} keeps a
   * neutral rename because jobs and toggles have no discriminator; here a rename that does not
   * state a type is exactly the operation this entity must not expose.
   */
  @Override
  default ApiResponse<Void> renameEntity(UserTable fromEntity, UserTable toEntity) {
    throw new UnsupportedOperationException("Use renameEntity(from, to, entityType)");
  }
}
