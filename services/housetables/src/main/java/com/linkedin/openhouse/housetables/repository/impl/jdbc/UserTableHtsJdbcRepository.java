package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.config.db.jdbc.JdbcProviderConfiguration;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * JDBC-backed {@link HtsRepository} for CRUDing {@link UserTableRow}
 *
 * <p>This class gets configured in {@link
 * com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration} with @EnableJpaRepositories.
 * The datasource for the Jpa repository is provided in {@link JdbcProviderConfiguration}.
 */
public interface UserTableHtsJdbcRepository
    extends HtsRepository<UserTableRow, UserTableRowPrimaryKey> {
  /**
   * Look up the entity in a case-insensitive way as a framework-provided feature. Details: 1. All
   * keys required in lookup need to be explicitly added in the arguments. Composite keys doesn't
   * work. 2. When naming the method, all keys that are used to looked-up in a case-insensitive way
   * need to be postfixed with `ignoreCase` explicitly.
   *
   * @param databaseId
   * @param tableId
   * @return The object {@link UserTableRow} looked-up in a case-insensitive way.
   */
  Optional<UserTableRow> findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      String databaseId, String tableId);

  boolean existsByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(String databaseId, String tableId);

  void deleteByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(String databaseId, String tableId);

  String TABLE_ROW_PREDICATE = "(u.entityType IS NULL OR upper(u.entityType) = 'TABLE')";

  /**
   * Table-scoped point read serving {@code getUserTable}, the single HTS endpoint behind every
   * table point read in the tables service. The neutral {@link
   * #findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase} above stays unfiltered because the writers
   * must see a row of any type to detect a collision at a shared key.
   */
  @Query(
      "SELECT u FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) = lower(:tableId) AND "
          + TABLE_ROW_PREDICATE)
  Optional<UserTableRow> findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableId") String tableId);

  @Query("SELECT DISTINCT databaseId FROM UserTableRow")
  Iterable<String> findAllDistinctDatabaseIds();

  @Query(
      "SELECT DISTINCT databaseId FROM UserTableRow u where "
          + "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId))")
  Page<String> findAllDistinctDatabaseIds(String databaseId, Pageable pageable);

  @Query(
      "SELECT u FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + TABLE_ROW_PREDICATE)
  Iterable<UserTableRow> findAllTablesByDatabaseIdIgnoreCase(
      @Param("databaseId") String databaseId);

  @Query(
      value =
          "SELECT u FROM UserTableRow u WHERE "
              + "lower(u.databaseId) = lower(:databaseId) AND "
              + TABLE_ROW_PREDICATE,
      countQuery =
          "SELECT COUNT(u) FROM UserTableRow u WHERE "
              + "lower(u.databaseId) = lower(:databaseId) AND "
              + TABLE_ROW_PREDICATE)
  Page<UserTableRow> findAllTablesByDatabaseIdIgnoreCase(
      @Param("databaseId") String databaseId, Pageable pageable);

  @Query(
      "SELECT u FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) LIKE lower(:tableIdPattern) AND "
          + TABLE_ROW_PREDICATE)
  Iterable<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableIdPattern") String tableIdPattern);

  @Query(
      value =
          "SELECT u FROM UserTableRow u WHERE "
              + "lower(u.databaseId) = lower(:databaseId) AND "
              + "lower(u.tableId) LIKE lower(:tableIdPattern) AND "
              + TABLE_ROW_PREDICATE,
      countQuery =
          "SELECT COUNT(u) FROM UserTableRow u WHERE "
              + "lower(u.databaseId) = lower(:databaseId) AND "
              + "lower(u.tableId) LIKE lower(:tableIdPattern) AND "
              + TABLE_ROW_PREDICATE)
  Page<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId,
      @Param("tableIdPattern") String tableIdPattern,
      Pageable pageable);

  /**
   * A null or {@code TABLE} request means tables, including legacy null rows; {@code VIEW} means
   * views only. An unknown value matches neither branch, so garbage fails closed here even if it
   * bypasses API validation.
   */
  String ENTITY_TYPE_FILTER_PREDICATE =
      "(((:entityType IS NULL OR upper(:entityType) = 'TABLE') AND "
          + TABLE_ROW_PREDICATE
          + ") OR (upper(:entityType) = 'VIEW' AND upper(u.entityType) = 'VIEW'))";

  String GENERAL_FILTER_PREDICATE =
      "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId)) AND "
          + "(:tableId IS NULL OR lower(u.tableId) = lower(:tableId)) AND "
          + "(:tableVersion IS NULL OR u.version = :tableVersion) AND "
          + "(:metadataLocation IS NULL OR u.metadataLocation = :metadataLocation) AND "
          + "(:storageType IS NULL OR u.storageType = :storageType) AND "
          + "(:creationTime IS NULL OR u.creationTime = :creationTime) AND "
          + ENTITY_TYPE_FILTER_PREDICATE;

  @Query(
      value = "select DISTINCT u from UserTableRow u where " + GENERAL_FILTER_PREDICATE,
      countQuery = "select COUNT(DISTINCT u) from UserTableRow u where " + GENERAL_FILTER_PREDICATE)
  Page<UserTableRow> findAllByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime,
      @Param("entityType") String entityType,
      Pageable pageable);

  @Query("select DISTINCT u from UserTableRow u where " + GENERAL_FILTER_PREDICATE)
  Iterable<UserTableRow> findAllByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime,
      @Param("entityType") String entityType);

  /*
   * The following methods are required to maintain the generality of the interface {@link com.linkedin.openhouse.housetables.repository.HtsRepository}
   */

  @Override
  default @NotNull Optional<UserTableRow> findById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    return findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        userTableRowPrimaryKey.getDatabaseId(), userTableRowPrimaryKey.getTableId());
  }

  @Override
  default boolean existsById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    return existsByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        userTableRowPrimaryKey.getDatabaseId(), userTableRowPrimaryKey.getTableId());
  }

  @Override
  default void deleteById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    deleteByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        userTableRowPrimaryKey.getDatabaseId(), userTableRowPrimaryKey.getTableId());
  }

  @Transactional
  @Modifying
  @Query(
      "UPDATE UserTableRow table SET table.tableId = :toTableId, table.metadataLocation = :metadataLocation, table.databaseId = :toDatabaseId "
          + "WHERE lower(table.databaseId) = lower(:fromDatabaseId) AND lower(table.tableId) = lower(:fromTableId)")
  void renameTableId(
      @Param("fromDatabaseId") String fromDatabaseId,
      @Param("fromTableId") String fromTableId,
      @Param("toDatabaseId") String toDatabaseId,
      @Param("toTableId") String toTableId,
      @Param("metadataLocation") String metadataLocation);
}
