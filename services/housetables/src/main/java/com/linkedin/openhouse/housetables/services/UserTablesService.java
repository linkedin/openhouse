package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.EntityType;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;

/** Service Interface for Implementing /hts/tables endpoint. */
public interface UserTablesService {
  /**
   * @param databaseId part of the primary composite key
   * @param tableId part of the primary composite key
   * @return {@link UserTableDto}. Avoid using {@link UserTable} directly for decoupling between
   *     service and transport layer.
   */
  UserTableDto getUserTable(String databaseId, String tableId);

  /**
   * Reads whatever occupies a (databaseId, tableId) key, of any type, and reports which type it is.
   * This is the occupancy primitive collision detection needs: "the name is free" is the dangerous
   * default, so absence must mean genuine absence. Repository and hydration failures propagate.
   *
   * @param databaseId part of the primary composite key
   * @param tableId part of the primary composite key
   * @return {@link UserTableDto} carrying a canonical, non-null entity type
   */
  UserTableDto getNeutralEntity(String databaseId, String tableId);

  /**
   * The view mirror of {@link #getUserTable}. A table or a legacy null at the key resolves as
   * absent, so a view caller needs no type check of its own.
   */
  UserTableDto getUserView(String databaseId, String tableId);

  /**
   * Given a partially filled {@link UserTable} object, prepare list of {@link UserTableDto}s that
   * matches with the provided {@link UserTable}. See
   * com.linkedin.openhouse.housetables.dto.model.UserTableDto#match for the definition of match.
   *
   * @param userTable object served as filtering condition.
   * @return list of {@link UserTableDto}s that matches the provided {@link UserTable}
   */
  List<UserTableDto> getAllUserTables(UserTable userTable);

  /**
   * Given a partially filled {@link UserTable} object, prepare a paginated {@link UserTableDto}s
   * that matches with the provided {@link UserTable}. See
   * com.linkedin.openhouse.housetables.dto.model.UserTableDto#match for the definition of match.
   *
   * @param userTable
   * @param page The page number to be retrieved
   * @param size The number of {@link UserTableDto}s in the specified page
   * @param sortBy The results sorted by field in {@link UserTable}. For example, tableId,
   *     databaseId
   * @return
   */
  Page<UserTableDto> getAllUserTables(UserTable userTable, int page, int size, String sortBy);

  /**
   * The view mirror of {@link #getAllUserTables(UserTable)}. An empty filter returns every view
   * rather than a projection of database names: database enumeration is type-agnostic and stays on
   * the table query, so mirroring it here would conflate objects with namespaces twice.
   */
  List<UserTableDto> getAllUserViews(UserTable userTable);

  /** The paged view mirror of {@link #getAllUserTables(UserTable, int, int, String)}. */
  Page<UserTableDto> getAllUserViews(UserTable userTable, int page, int size, String sortBy);

  /** Given a databaseId and tableId, delete the user table entry from the House Table. */
  void deleteUserTable(String databaseId, String tableId, boolean isSoftDelete);

  /**
   * Given a databaseId and tableId, hard-delete the view entry from the House Table. Views have no
   * soft delete: {@code soft_deleted_user_table_row} carries no discriminator, so a view routed
   * through it would restore as a table.
   */
  void deleteUserView(String databaseId, String tableId);

  /**
   * Create or update a {@link UserTable} row in House table.
   *
   * @param userTable The object attempted to be used for update/creation.
   * @return A pair of object: The first {@link UserTableDto} is the actual saved object. The second
   *     boolean is set to true if overwritten occurred. This is to differentiate between creation
   *     and update of {@link UserTableDto}.
   */
  Pair<UserTableDto, Boolean> putUserTable(UserTable userTable);

  /**
   * Rename a {@link UserTable} row in House table.
   *
   * @param fromDatabaseId The databaseId of the row to rename.
   * @param fromTableId The tableId of the row to rename.
   * @param toDatabaseId The new databaseId of the renamed row.
   * @param toTableId The new tableId of the renamed row.
   * @param metadataLocation The new metadata file of the table with updated table properties for
   *     updated ids.
   * @param entityType The type the calling route serves. Internally bound by the controller, never
   *     supplied by a caller, so a rename scoped to one type cannot move a row of the other.
   */
  void renameUserTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation,
      EntityType entityType);

  /**
   * Restore a soft-deleted user table identified by its databaseId, tableId, and deletedAtMs
   *
   * @param databaseId
   * @param tableId
   * @param deletedAtMs
   */
  UserTableDto restoreUserTable(String databaseId, String tableId, Long deletedAtMs);

  /**
   * Delete soft deleted user tables given a databaseId, tableId that are older than the given
   * purgeAfterMs
   *
   * @param databaseId
   * @param tableId
   * @param purgeAfterMs
   */
  void purgeSoftDeletedUserTables(String databaseId, String tableId, Long purgeAfterMs);

  /**
   * Get all soft deleted tables by filters.
   *
   * <p>Currently the filters supported are limited to databaseId, tableId, and purgeAfterMs.
   *
   * @param userTable
   * @param page
   * @param size
   * @param sortBy
   * @return
   */
  Page<UserTableDto> getAllSoftDeletedTables(
      UserTable userTable, int page, int size, String sortBy);
}
