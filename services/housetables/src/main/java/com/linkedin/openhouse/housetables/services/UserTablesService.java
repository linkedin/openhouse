package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
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

  /** Given a databaseId and tableId, delete the user table entry from the House Table. */
  void deleteUserTable(String databaseId, String tableId);

  /**
   * Create or update a {@link UserTable} row in House table.
   *
   * @param userTable The object attempted to be used for update/creation.
   * @return A pair of object: The first {@link UserTableDto} is the actual saved object. The second
   *     boolean is set to true if overwritten occurred. This is to differentiate between creation
   *     and update of {@link UserTableDto}.
   */
  Pair<UserTableDto, Boolean> putUserTable(UserTable userTable);
}
