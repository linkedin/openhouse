package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.config.db.jdbc.JdbcProviderConfiguration;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.Iterator;
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

  String TABLE_ENTITY_TYPE = "TABLE";

  /**
   * {@code null} matches any type. {@code TABLE} also matches a stored null, because the column is
   * nullable with no backfill and an absent discriminator means a table. An unrecognized request
   * value matches neither branch, so garbage fails closed.
   */
  String ENTITY_TYPE_PREDICATE =
      "(:entityType IS NULL "
          + "OR (upper(:entityType) = 'TABLE' "
          + "AND (u.entityType IS NULL OR upper(u.entityType) = 'TABLE')) "
          + "OR (upper(:entityType) = 'VIEW' AND upper(u.entityType) = 'VIEW'))";

  @Query("SELECT DISTINCT databaseId FROM UserTableRow")
  Iterable<String> findAllDistinctDatabaseIds();

  @Query(
      "SELECT DISTINCT databaseId FROM UserTableRow u where "
          + "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId))")
  Page<String> findAllDistinctDatabaseIds(String databaseId, Pageable pageable);

  String PATTERN_FILTER_PREDICATE =
      "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) LIKE lower(:tableIdPattern) AND "
          + ENTITY_TYPE_PREDICATE;

  /**
   * Kept separate from {@link #findAllByFilters} because that query matches {@code tableId}
   * exactly. Folding a LIKE into it would make {@code _} a wildcard, and OpenHouse identifiers
   * routinely contain underscores.
   */
  @Query("SELECT u FROM UserTableRow u WHERE " + PATTERN_FILTER_PREDICATE)
  Iterable<UserTableRow> findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId,
      @Param("tableIdPattern") String tableIdPattern,
      @Param("entityType") String entityType);

  @Query(
      value = "SELECT u FROM UserTableRow u WHERE " + PATTERN_FILTER_PREDICATE,
      countQuery = "SELECT COUNT(u) FROM UserTableRow u WHERE " + PATTERN_FILTER_PREDICATE)
  Page<UserTableRow> findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId,
      @Param("tableIdPattern") String tableIdPattern,
      @Param("entityType") String entityType,
      Pageable pageable);

  String GENERAL_FILTER_PREDICATE =
      "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId)) AND "
          + "(:tableId IS NULL OR lower(u.tableId) = lower(:tableId)) AND "
          + "(:tableVersion IS NULL OR u.version = :tableVersion) AND "
          + "(:metadataLocation IS NULL OR u.metadataLocation = :metadataLocation) AND "
          + "(:storageType IS NULL OR u.storageType = :storageType) AND "
          + "(:creationTime IS NULL OR u.creationTime = :creationTime) AND "
          + ENTITY_TYPE_PREDICATE;

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
   * Table-scoped views onto the general queries above. They pin the discriminator and own no JPQL,
   * so a table caller cannot drift from the general semantics.
   */

  default Page<UserTableRow> findAllTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime,
      Pageable pageable) {
    return findAllByFilters(
        databaseId,
        tableId,
        tableVersion,
        metadataLocation,
        storageType,
        creationTime,
        TABLE_ENTITY_TYPE,
        pageable);
  }

  default Iterable<UserTableRow> findAllTablesByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime) {
    return findAllByFilters(
        databaseId,
        tableId,
        tableVersion,
        metadataLocation,
        storageType,
        creationTime,
        TABLE_ENTITY_TYPE);
  }

  default Iterable<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      String databaseId, String tableIdPattern) {
    return findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
        databaseId, tableIdPattern, TABLE_ENTITY_TYPE);
  }

  default Page<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      String databaseId, String tableIdPattern, Pageable pageable) {
    return findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
        databaseId, tableIdPattern, TABLE_ENTITY_TYPE, pageable);
  }

  /**
   * Table-scoped point read serving {@code getUserTable}, the single HTS endpoint behind every
   * table point read in the tables service. The key is the primary key, so at most one row can
   * match. The neutral {@link #findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase} stays unfiltered
   * because the writers must see a row of any type to detect a collision at a shared key.
   */
  default Optional<UserTableRow> findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      String databaseId, String tableId) {
    Iterator<UserTableRow> matches =
        findAllTablesByFilters(databaseId, tableId, null, null, null, null).iterator();
    return matches.hasNext() ? Optional.of(matches.next()) : Optional.empty();
  }

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
