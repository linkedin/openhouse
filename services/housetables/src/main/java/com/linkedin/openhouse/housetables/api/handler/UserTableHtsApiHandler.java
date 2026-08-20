package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.EntityType;

/**
 * Invocation of generic type {@link HouseTablesApiHandler} using {@link UserTable} as the entity
 * type. Type is selected by which method is called, never by an argument; {@link #renameEntity} is
 * the one exception, and its type is bound by the controller rather than by a caller.
 */
public interface UserTableHtsApiHandler extends HouseTablesApiHandler<UserTableKey, UserTable> {

  /**
   * Deletes a row given its key. The soft-delete flag is table-only; {@link #deleteView} has no
   * equivalent because views have no soft-deleted store.
   */
  ApiResponse<Void> deleteEntity(UserTableKey key, boolean isSoftDelete);

  /** The occupancy read: whatever holds the key, of any type. Not a polymorphic catalog lookup. */
  ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey key);

  ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey key);

  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable entity);

  ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable entity, int page, int size, String sortBy);

  ApiResponse<EntityResponseBody<UserTable>> putView(UserTable entity);

  /** Always hard: views have no soft-deleted store, hence no soft-delete flag. */
  ApiResponse<Void> deleteView(UserTableKey key);

  /** Only TABLE is ever passed: views are not renameable in M1, so no view rename route exists. */
  ApiResponse<Void> renameEntity(UserTable fromEntity, UserTable toEntity, EntityType entityType);

  /**
   * Sealed in favour of the typed overload. The shared {@link HouseTablesApiHandler} keeps a
   * neutral rename because jobs and toggles have no discriminator; for this entity a rename that
   * does not state a type is exactly what must not be exposed.
   */
  @Override
  default ApiResponse<Void> renameEntity(UserTable fromEntity, UserTable toEntity) {
    throw new UnsupportedOperationException("Use renameEntity(from, to, entityType)");
  }
}
