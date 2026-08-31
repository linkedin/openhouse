package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.exception.UserTableReadException;
import com.linkedin.openhouse.housetables.services.model.PagedUserViewQuery;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.util.Pair;

/**
 * Service Interface for Implementing /hts/tables endpoint.
 *
 * <p>The neutral and view methods report absence as an empty {@link Optional} and nothing else: a
 * repository or hydration failure surfaces as {@link CorruptUserTableDataException} or {@link
 * UserTableReadException}, because reporting an unreadable row as absent is how an occupant gets
 * overwritten.
 */
public interface UserTablesService {
  /**
   * @param databaseId part of the primary composite key
   * @param tableId part of the primary composite key
   * @return {@link UserTableDto}. Avoid using {@link UserTable} directly for decoupling between
   *     service and transport layer.
   */
  UserTableDto getUserTable(String databaseId, String tableId);

  /** Reads the occupant of a key whatever its type, for collision detection. */
  Optional<UserTableDto> getNeutralEntity(String databaseId, String tableId);

  /** A table or a legacy null at the key resolves as absent. */
  Optional<UserTableDto> getUserView(String databaseId, String tableId);

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
   * Unlike {@link #getAllUserTables(UserTable)}, an empty query returns every view rather than a
   * projection of database names; database enumeration stays type-agnostic on the table query.
   */
  List<UserTableDto> getAllUserViews(UserViewQuery query);

  Page<UserTableDto> getAllUserViews(PagedUserViewQuery query);

  /** Given a databaseId and tableId, delete the user table entry from the House Table. */
  void deleteUserTable(String databaseId, String tableId, boolean isSoftDelete);

  /**
   * Always a hard delete: {@code soft_deleted_user_table_row} carries no discriminator, so a view
   * routed through it would restore as a table.
   *
   * @return false when no view held the key, covering absence and a wrong-type occupant alike
   */
  boolean deleteUserView(String databaseId, String tableId);

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
   * Create or update a view row in House table. The method, not the payload, is the authority on
   * the type: a transport {@code entityType} is overwritten with VIEW before the row is saved.
   *
   * @return the saved DTO, and true when an existing view was overwritten rather than created
   */
  Pair<UserTableDto, Boolean> putUserView(UserTable userView);

  /**
   * Rename a {@link UserTable} row in House table.
   *
   * @param fromDatabaseId The databaseId of the row to rename.
   * @param fromTableId The tableId of the row to rename.
   * @param toDatabaseId The new databaseId of the renamed row.
   * @param toTableId The new tableId of the renamed row.
   * @param metadataLocation The new metadata file of the table with updated table properties for
   *     updated ids.
   */
  void renameUserTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation);

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
