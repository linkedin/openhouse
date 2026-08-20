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

  /**
   * A legacy row predates the discriminator and is definitively a table, so the null arm is load
   * bearing: without it every pre-existing table becomes invisible. Do not simplify it to a plain
   * equality before a verified backfill and a {@code NOT NULL} migration.
   *
   * <p>Collation caveat, applying equally to {@link #VIEW_ROW_PREDICATE}: this compares against a
   * string literal, so the comparison inherits the column's collation. Production MySQL's default
   * {@code utf8_unicode_ci} is accent-insensitive and ignores trailing spaces, meaning a stored
   * {@code 'TÁBLE'} or {@code 'TABLE '} would be matched here while {@link
   * com.linkedin.openhouse.housetables.model.EntityType#fromName} rejects both. Such a row is
   * therefore reachable by the bulk delete and rename statements, which never hydrate, yet fails
   * every read. H2 in {@code MODE=MySQL} does not reproduce this, so no test pins it. It is left as
   * a known limitation rather than fixed: a binary-exact comparison is not expressible in JPQL and
   * would mean dropping to native SQL for twelve composed queries, while the exposure is narrow —
   * {@code ValidatorConstants.ENTITY_TYPE_REGEX} rejects such a value on every endpoint, so only
   * direct database manipulation can create one, and no collation makes VIEW match TABLE, so
   * cross-type deletion stays impossible.
   */
  String TABLE_ROW_PREDICATE = "(u.entityType IS NULL OR upper(u.entityType) = 'TABLE')";

  /**
   * Unlike {@link #TABLE_ROW_PREDICATE} this is a plain equality: there is no legacy-null case to
   * absorb, because a null column predates the discriminator and is definitively a table. The
   * collation caveat documented on that constant applies here too.
   */
  String VIEW_ROW_PREDICATE = "upper(u.entityType) = 'VIEW'";

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

  /**
   * View-scoped point read serving {@code getUserView}, the mirror of {@link
   * #findTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase}. A table or a legacy null at the same key
   * resolves to empty, so the view lifecycle treats it as absent without a check of its own.
   */
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
   * Deletes only a row whose entity_type is NULL or TABLE. A VIEW at the same key is left
   * untouched. Returns the affected-row count so the service can map missing or wrong-type deletion
   * to 404. Do not simplify this to a neutral delete: neutral key mutation is exactly what this
   * ticket removes.
   */
  default int deleteTableById(UserTableRowPrimaryKey key) {
    return deleteTableByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

  /**
   * Deletes only a row whose entity_type is VIEW. A TABLE or legacy NULL row at the same key is
   * left untouched. Returns the affected-row count so the service can map missing or wrong-type
   * deletion to 404. Do not simplify this to a neutral delete: neutral key mutation is exactly what
   * this ticket removes.
   */
  default int deleteViewById(UserTableRowPrimaryKey key) {
    return deleteViewByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        key.getDatabaseId(), key.getTableId());
  }

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

  /**
   * Table-scoped, and the type it stamps is bound by the caller rather than inlined as a literal,
   * so the controller stays the one place that decides which entity a route operates on. Returns
   * the affected-row count: zero means the source is absent or is not a table, which the service
   * maps to 404, and a destination collision surfaces as a primary-key violation instead.
   */
  @Transactional
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(
      "UPDATE UserTableRow u SET "
          + "u.tableId = :toTableId, "
          + "u.metadataLocation = :metadataLocation, "
          + "u.databaseId = :toDatabaseId, "
          + "u.entityType = :entityType "
          + "WHERE lower(u.databaseId) = lower(:fromDatabaseId) "
          + "AND lower(u.tableId) = lower(:fromTableId) AND "
          + TABLE_ROW_PREDICATE)
  int renameTableId(
      @Param("fromDatabaseId") String fromDatabaseId,
      @Param("fromTableId") String fromTableId,
      @Param("toDatabaseId") String toDatabaseId,
      @Param("toTableId") String toTableId,
      @Param("metadataLocation") String metadataLocation,
      @Param("entityType") EntityType entityType);
}
