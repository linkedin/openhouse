package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.config.db.jdbc.JdbcProviderConfiguration;
import com.linkedin.openhouse.housetables.model.EntityType;
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
   * <p>Collation assumption, shared with {@link #VIEW_ROW_PREDICATE}: this comparison assumes the
   * column's collation matches these values exactly, folding neither accents nor trailing spaces,
   * so that SQL agrees with {@link EntityType#fromName} about what counts as a table. An accent
   * insensitive collation would let a stored {@code 'TÁBLE'} match here yet fail hydration, and a
   * PAD SPACE collation would do the same for {@code 'TABLE '}. Unconfirmed against production;
   * please confirm the deployed collation.
   */
  String TABLE_ROW_PREDICATE = "(u.entityType IS NULL OR upper(u.entityType) = '" + TABLE + "')";

  String VIEW_ROW_PREDICATE = "upper(u.entityType) = '" + VIEW + "'";

  String PATTERN_KEY_CLAUSES =
      "lower(u.databaseId) = lower(:databaseId) AND "
          + "lower(u.tableId) LIKE lower(:tableIdPattern)";

  /** The neutral finder above stays unfiltered so writers can spot a collision at a shared key. */
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
   * Deletes only a NULL or TABLE row, leaving a VIEW at the same key untouched, and returns the
   * affected-row count so the service maps missing and wrong-type alike to 404. Deliberately not a
   * neutral delete: the soft-deleted store has no discriminator and must never receive a view.
   */
  default int deleteTableById(UserTableRowPrimaryKey key) {
    return deleteTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

  /** The mirror of {@link #deleteTableById}: only a VIEW row, never a TABLE or legacy NULL. */
  default int deleteViewById(UserTableRowPrimaryKey key) {
    return deleteViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

  /**
   * The key-addressed generic deletes are sealed because a wrong-type delete is irreversible.
   * No-arg {@code deleteAll()} stays available: it addresses no key, and test teardown depends on
   * it.
   */
  @Override
  default void deleteById(UserTableRowPrimaryKey key) {
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

  /** Table-only: views are not renameable, so there is deliberately no {@code renameViewId}. */
  @Transactional
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE UserTableRow u SET "
          + "u.tableId = :toTableId, "
          + "u.metadataLocation = :metadataLocation, "
          + "u.databaseId = :toDatabaseId, "
          + "u.entityType = '"
          + TABLE
          + "' "
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
