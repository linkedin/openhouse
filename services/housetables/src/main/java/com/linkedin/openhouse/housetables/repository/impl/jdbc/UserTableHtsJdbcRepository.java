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
 *
 * <p>Every method here is advised by {@link UserTableRepositoryTranslationAspect}, so a corrupt
 * discriminator surfaces with its diagnostic whoever calls it. A read added later needs nothing at
 * its call site.
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

  String COMMON_FILTER_CLAUSES =
      "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId)) AND "
          + "(:tableId IS NULL OR lower(u.tableId) = lower(:tableId)) AND "
          + "(:tableVersion IS NULL OR u.version = :tableVersion) AND "
          + "(:metadataLocation IS NULL OR u.metadataLocation = :metadataLocation) AND "
          + "(:storageType IS NULL OR u.storageType = :storageType) AND "
          + "(:creationTime IS NULL OR u.creationTime = :creationTime)";

  String TABLE = "TABLE";

  String VIEW = "VIEW";

  /**
   * The null arm is load bearing: a legacy row predates the discriminator and is definitively a
   * table, so without it every pre-existing table becomes invisible. Do not reduce it to a plain
   * equality before a verified backfill and a {@code NOT NULL} migration.
   *
   * <p>Production collation is confirmed {@code utf8mb4_0900_ai_ci}: accent-insensitive and NO PAD.
   * A stored {@code 'TÁBLE'} therefore matches this predicate but fails {@code EntityType#fromName}
   * on hydration (a 500), and a {@code 'TABLE '} does not match at all. Both require a direct
   * database write; the API writes only canonical enum names, so neither is a reachable state.
   */
  String TABLE_ROW_PREDICATE = "(u.entityType IS NULL OR upper(u.entityType) = '" + TABLE + "')";

  String VIEW_ROW_PREDICATE = "upper(u.entityType) = '" + VIEW + "'";

  String PATTERN_KEY_CLAUSES =
      "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) LIKE lower(:tableIdPattern)";

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

  Iterable<UserTableRow> findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
      String databaseId, String tableIdPattern);

  @Query(
      "SELECT DISTINCT databaseId FROM UserTableRow u where "
          + "(:databaseId IS NULL OR lower(u.databaseId) = lower(:databaseId))")
  Page<String> findAllDistinctDatabaseIds(String databaseId, Pageable pageable);

  Page<UserTableRow> findAllByDatabaseIdAndTableIdLikeAllIgnoreCase(
      String databaseId, String tableIdPattern, Pageable pageable);

  @Query("select DISTINCT u from UserTableRow u where " + COMMON_FILTER_CLAUSES)
  Page<UserTableRow> findAllByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime,
      Pageable pageable);

  @Query("select DISTINCT u from UserTableRow u where " + COMMON_FILTER_CLAUSES)
  Iterable<UserTableRow> findAllByFilters(
      String databaseId,
      String tableId,
      String tableVersion,
      String metadataLocation,
      String storageType,
      Long creationTime);

  @Query(
      "SELECT u FROM UserTableRow u WHERE " + PATTERN_KEY_CLAUSES + " AND " + TABLE_ROW_PREDICATE)
  Iterable<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableIdPattern") String tableIdPattern);

  @Query(
      value =
          "SELECT u FROM UserTableRow u WHERE "
              + PATTERN_KEY_CLAUSES
              + " AND "
              + TABLE_ROW_PREDICATE,
      countQuery =
          "SELECT COUNT(u) FROM UserTableRow u WHERE "
              + PATTERN_KEY_CLAUSES
              + " AND "
              + TABLE_ROW_PREDICATE)
  Page<UserTableRow> findAllTablesByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId,
      @Param("tableIdPattern") String tableIdPattern,
      Pageable pageable);

  @Query(
      value =
          "select DISTINCT u from UserTableRow u where "
              + COMMON_FILTER_CLAUSES
              + " AND "
              + TABLE_ROW_PREDICATE,
      countQuery =
          "select COUNT(DISTINCT u) from UserTableRow u where "
              + COMMON_FILTER_CLAUSES
              + " AND "
              + TABLE_ROW_PREDICATE)
  Page<UserTableRow> findAllTablesByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime,
      Pageable pageable);

  @Query(
      "select DISTINCT u from UserTableRow u where "
          + COMMON_FILTER_CLAUSES
          + " AND "
          + TABLE_ROW_PREDICATE)
  Iterable<UserTableRow> findAllTablesByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime);

  // view-scoped reads: the table set mirrored against the same constants

  @Query(
      "SELECT u FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) = lower(:tableId) AND "
          + VIEW_ROW_PREDICATE)
  Optional<UserTableRow> findViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableId") String tableId);

  @Query(
      "select DISTINCT u from UserTableRow u where "
          + COMMON_FILTER_CLAUSES
          + " AND "
          + VIEW_ROW_PREDICATE)
  Iterable<UserTableRow> findAllViewsByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime);

  @Query(
      value =
          "select DISTINCT u from UserTableRow u where "
              + COMMON_FILTER_CLAUSES
              + " AND "
              + VIEW_ROW_PREDICATE,
      countQuery =
          "select COUNT(DISTINCT u) from UserTableRow u where "
              + COMMON_FILTER_CLAUSES
              + " AND "
              + VIEW_ROW_PREDICATE)
  Page<UserTableRow> findAllViewsByFilters(
      @Param("databaseId") String databaseId,
      @Param("tableId") String tableId,
      @Param("tableVersion") String tableVersion,
      @Param("metadataLocation") String metadataLocation,
      @Param("storageType") String storageType,
      @Param("creationTime") Long creationTime,
      Pageable pageable);

  @Query("SELECT u FROM UserTableRow u WHERE " + PATTERN_KEY_CLAUSES + " AND " + VIEW_ROW_PREDICATE)
  Iterable<UserTableRow> findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableIdPattern") String tableIdPattern);

  @Query(
      value =
          "SELECT u FROM UserTableRow u WHERE "
              + PATTERN_KEY_CLAUSES
              + " AND "
              + VIEW_ROW_PREDICATE,
      countQuery =
          "SELECT COUNT(u) FROM UserTableRow u WHERE "
              + PATTERN_KEY_CLAUSES
              + " AND "
              + VIEW_ROW_PREDICATE)
  Page<UserTableRow> findAllViewsByDatabaseIdAndTableIdLikeAllIgnoreCase(
      @Param("databaseId") String databaseId,
      @Param("tableIdPattern") String tableIdPattern,
      Pageable pageable);

  // type-scoped deletion: one conditional statement, never read-then-delete

  /** Bulk statements bypass the persistence context, hence the flush and clear. */
  @Transactional
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "DELETE FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) = lower(:tableId) AND "
          + TABLE_ROW_PREDICATE)
  int deleteTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableId") String tableId);

  @Transactional
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "DELETE FROM UserTableRow u WHERE "
          + "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) = lower(:tableId) AND "
          + VIEW_ROW_PREDICATE)
  int deleteViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      @Param("databaseId") String databaseId, @Param("tableId") String tableId);

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

  /**
   * Sealed because a wrong-type delete is irreversible, and because falling through to {@code
   * SimpleJpaRepository} would silently change the case and absence semantics of the derived
   * neutral delete this change removed. No-arg {@code deleteAll()} is sealed for the same reason:
   * nothing in production wipes the store, and test teardown is not a reason to publish it.
   */
  @Override
  default void deleteById(UserTableRowPrimaryKey userTableRowPrimaryKey) {
    throw new UnsupportedOperationException("Use deleteTableById or deleteViewById");
  }

  @Override
  default void delete(UserTableRow entity) {
    throw new UnsupportedOperationException("Use deleteTableById or deleteViewById");
  }

  @Override
  default void deleteAllById(Iterable<? extends UserTableRowPrimaryKey> keys) {
    throw new UnsupportedOperationException("Use typed single-entity deletion");
  }

  @Override
  default void deleteAll(Iterable<? extends UserTableRow> entities) {
    throw new UnsupportedOperationException("Use typed single-entity deletion");
  }

  @Override
  default void deleteAll() {
    throw new UnsupportedOperationException("Use typed single-entity deletion");
  }

  /**
   * Returns the affected-row count so the service maps missing and wrong-type alike to 404.
   * Deliberately not neutral: the soft-deleted store has no discriminator and must never take a
   * view.
   */
  default int deleteTableById(UserTableRowPrimaryKey key) {
    return deleteTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

  default int deleteViewById(UserTableRowPrimaryKey key) {
    return deleteViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

  String STAMP_TABLE_TYPE = "u.entityType = '" + TABLE + "' ";

  /**
   * Table-only: views are not renameable. The conditional update is itself the source check, and
   * {@link #STAMP_TABLE_TYPE} rewrites a legacy null or non-canonical spelling as the row moves.
   */
  @Transactional
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE UserTableRow u SET "
          + "u.tableId = :toTableId, "
          + "u.metadataLocation = :metadataLocation, "
          + "u.databaseId = :toDatabaseId, "
          + STAMP_TABLE_TYPE
          + "WHERE lower(u.databaseId) = lower(:fromDatabaseId) "
          + "AND lower(u.tableId) = lower(:fromTableId) AND "
          + TABLE_ROW_PREDICATE)
  int renameTableId(
      @Param("fromDatabaseId") String fromDatabaseId,
      @Param("fromTableId") String fromTableId,
      @Param("toDatabaseId") String toDatabaseId,
      @Param("toTableId") String toTableId,
      @Param("metadataLocation") String metadataLocation);
}
