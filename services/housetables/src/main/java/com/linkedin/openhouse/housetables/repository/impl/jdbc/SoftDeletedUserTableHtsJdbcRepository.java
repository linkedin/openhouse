package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.config.db.jdbc.JdbcProviderConfiguration;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

/**
 * JDBC-backed {@link HtsRepository} for CRUDing {@link UserTableRow}
 *
 * <p>This class gets configured in {@link
 * com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration} with @EnableJpaRepositories.
 * The datasource for the Jpa repository is provided in {@link JdbcProviderConfiguration}.
 */
public interface SoftDeletedUserTableHtsJdbcRepository
    extends HtsRepository<SoftDeletedUserTableRow, SoftDeletedUserTableRowPrimaryKey> {

  /**
   * Find a soft deleted table row uniquely identified by databaseId, tableId, and deletedAt
   * timestamp
   *
   * @param databaseId
   * @param tableId
   * @param deletedAt
   * @return
   */
  @Query(
      "SELECT u FROM SoftDeletedUserTableRow u"
          + " WHERE lower(u.databaseId) = lower(:databaseId)"
          + " AND lower(u.tableId) = lower(:tableId)"
          + " AND u.deletedAtMs = :deletedAt")
  Optional<SoftDeletedUserTableRow> findByDatabaseIdTableIdDeletedAt(
      String databaseId, String tableId, Long deletedAt);

  /**
   * Find all soft deleted tables by any combination of filters. If purgeAfterMs is provided, it
   * will return all soft deleted tables that expire before the given timestamp
   *
   * @param databaseId
   * @param tableId
   * @param purgeAfterMs
   * @param pageable
   * @return
   */
  @Query(
      "select DISTINCT u from SoftDeletedUserTableRow u where "
          + "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId)) AND "
          + "(:tableId IS NULL OR lower(u.tableId) = lower(:tableId)) AND "
          + "(:purgeAfterMs IS NULL OR u.purgeAfterMs < :purgeAfterMs)")
  Page<SoftDeletedUserTableRow> findAllByFilters(
      String databaseId, String tableId, Long purgeAfterMs, Pageable pageable);

  /**
   * Delete a soft deleted table row uniquely identified by databaseId, tableId, and deletedAt
   * timestamp Used for tests
   *
   * @param databaseId
   * @param tableId
   * @param deletedAt
   */
  @Query(
      "DELETE FROM SoftDeletedUserTableRow u"
          + " WHERE lower(u.databaseId) = lower(:databaseId)"
          + " AND lower(u.tableId) = lower(:tableId)"
          + " AND u.deletedAtMs = :deletedAt")
  @Modifying
  void deleteByDatabaseIdTableIdDeletedAt(String databaseId, String tableId, Long deletedAt);

  /**
   * Delete all soft deleted table rows for a given databaseId and tableId that have a purgeAfterMs
   * that is before purgeFromMs
   *
   * @param databaseId
   * @param tableId
   * @param purgeFromMs
   */
  @Transactional
  @Query(
      "DELETE FROM SoftDeletedUserTableRow u"
          + " WHERE lower(u.databaseId) = lower(:databaseId)"
          + " AND lower(u.tableId) = lower(:tableId)"
          + " AND u.purgeAfterMs < :purgeFromMs")
  @Modifying
  void deleteByDatabaseIdTableIdPurgeAfterMs(String databaseId, String tableId, Long purgeFromMs);

  /**
   * Delete all soft deleted table rows for a given databaseId and tableId
   *
   * @param databaseId
   * @param tableId
   */
  @Transactional
  @Query(
      "DELETE FROM SoftDeletedUserTableRow u"
          + " WHERE lower(u.databaseId) = lower(:databaseId)"
          + " AND lower(u.tableId) = lower(:tableId)")
  @Modifying
  void deleteAllByDatabaseIdTableId(String databaseId, String tableId);

  /**
   * Check if a soft deleted table row exists uniquely identified by databaseId, tableId, and
   * deletedAt timestamp
   *
   * @param databaseId
   * @param tableId
   * @param deletedAt
   * @return
   */
  @Query(
      "SELECT CASE WHEN COUNT(u) > 0 THEN true ELSE false END FROM SoftDeletedUserTableRow u"
          + " WHERE lower(u.databaseId) = lower(:databaseId)"
          + " AND lower(u.tableId) = lower(:tableId)"
          + " AND u.deletedAtMs = :deletedAt")
  boolean existsByDatabaseIdTableIdDeletedAt(String databaseId, String tableId, Long deletedAt);

  /*
   * The following methods are required to maintain the generality of the interface {@link com.linkedin.openhouse.housetables.repository.SoftDeletedHtsRepository}
   */
  @Override
  default @NotNull Optional<SoftDeletedUserTableRow> findById(
      SoftDeletedUserTableRowPrimaryKey userTableRowPrimaryKey) {
    return findByDatabaseIdTableIdDeletedAt(
        userTableRowPrimaryKey.getDatabaseId(),
        userTableRowPrimaryKey.getTableId(),
        userTableRowPrimaryKey.getDeletedAtMs());
  }

  @Override
  default void deleteById(SoftDeletedUserTableRowPrimaryKey softDeletedUserTableRowPrimaryKey) {
    deleteByDatabaseIdTableIdDeletedAt(
        softDeletedUserTableRowPrimaryKey.getDatabaseId(),
        softDeletedUserTableRowPrimaryKey.getTableId(),
        softDeletedUserTableRowPrimaryKey.getDeletedAtMs());
  }

  @Override
  default boolean existsById(SoftDeletedUserTableRowPrimaryKey softDeletedUserTableRowPrimaryKey) {
    return existsByDatabaseIdTableIdDeletedAt(
        softDeletedUserTableRowPrimaryKey.getDatabaseId(),
        softDeletedUserTableRowPrimaryKey.getTableId(),
        softDeletedUserTableRowPrimaryKey.getDeletedAtMs());
  }
}
