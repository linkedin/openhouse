package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;

/**
 * Invocation of generic type {@link HouseTablesApiHandler} using {@link UserTable} as the entity
 * type. Type is selected by which method is called, never by an argument: an unqualified "entity"
 * denotes a table, view-scoped operations say so in their name, and {@link #getNeutralEntity} is
 * the one operation spanning both.
 *
 * <p>Query transport stops here. A query-shaped {@link UserTable} is validated and converted into
 * an owned service query value by the implementation, so the service never receives one.
 */
public interface UserTableHtsApiHandler extends HouseTablesApiHandler<UserTableKey, UserTable> {

  /**
   * The soft-delete flag is table-only; {@link #deleteView} has no equivalent because views have no
   * soft-deleted store.
   */
  ApiResponse<Void> deleteEntity(UserTableKey key, boolean isSoftDelete);

  /** Reads whatever occupies the key, of either type, for collision detection. */
  ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey key);

  ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey key);

  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable userView);

  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable userView, int page, int size, String sortBy);

  ApiResponse<EntityResponseBody<UserTable>> putView(UserTable userView);

  ApiResponse<Void> deleteView(UserTableKey key);

  /** Views are not renameable, so there is deliberately no {@code renameView}. */
  @Override
  ApiResponse<Void> renameEntity(UserTable fromEntity, UserTable toEntity);
}
