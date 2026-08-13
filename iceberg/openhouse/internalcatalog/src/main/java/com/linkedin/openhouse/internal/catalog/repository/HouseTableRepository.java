package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Base interface for repository backed by HouseTableService for storing and retrieving {@link
 * HouseTable} object.
 */
@Repository
public interface HouseTableRepository
    extends PagingAndSortingRepository<HouseTable, HouseTablePrimaryKey> {

  /**
   * Excludes views from table listings. The predicate lives in the query — never in a stream over a
   * returned {@link Page} — so content and counts agree. {@code IS NULL} is mandatory because the
   * discriminator is nullable with no backfill; {@code upper(...)} avoids depending on collation.
   *
   * <p>Spring Data proxies in services/tables and in the published tables-test-fixtures module
   * inherit these predicates without an edit.
   */
  String TABLE_ROW_PREDICATE = "(h.entityType IS NULL OR upper(h.entityType) = 'TABLE')";

  @Query("SELECT h FROM HouseTable h WHERE h.databaseId = :databaseId AND " + TABLE_ROW_PREDICATE)
  List<HouseTable> findAllByDatabaseId(@Param("databaseId") String databaseId);

  /**
   * Delete a table by its primary key with purge option
   *
   * @param houseTablePrimaryKey the primary key of the table
   * @param purge true if table should be deleted permanently, otherwise retain with soft delete
   */
  void deleteById(HouseTablePrimaryKey houseTablePrimaryKey, boolean purge);

  @Query(
      value =
          "SELECT h FROM HouseTable h WHERE h.databaseId = :databaseId AND " + TABLE_ROW_PREDICATE,
      countQuery =
          "SELECT COUNT(h) FROM HouseTable h WHERE h.databaseId = :databaseId AND "
              + TABLE_ROW_PREDICATE)
  Page<HouseTable> findAllByDatabaseId(@Param("databaseId") String databaseId, Pageable pageable);

  /** Redeclared only to add the table-only predicate; cardinality and dedup are unchanged. */
  @Override
  @Query("SELECT h FROM HouseTable h WHERE " + TABLE_ROW_PREDICATE)
  Iterable<HouseTable> findAll();

  @Override
  @Query(
      value = "SELECT h FROM HouseTable h WHERE " + TABLE_ROW_PREDICATE,
      countQuery = "SELECT COUNT(h) FROM HouseTable h WHERE " + TABLE_ROW_PREDICATE)
  Page<HouseTable> findAll(Pageable pageable);

  void rename(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation);

  /**
   * Find all soft-deleted tables by database ID with pagination and optional filtering
   *
   * @param databaseId The database ID to filter by
   * @param tableId The table ID to filter by (optional, can be null)
   * @param pageable Pagination information
   * @return List of soft-deleted HouseTable objects matching the criteria
   */
  Page<HouseTable> searchSoftDeletedTables(String databaseId, String tableId, Pageable pageable);

  /**
   * Delete soft-deleted tables that are older than the specified timestamp.
   *
   * @param databaseId
   * @param tableId
   * @param purgeAfterMs timestamp in milliseconds where tables older than this will be permanently
   *     deleted
   */
  void purgeSoftDeletedTables(String databaseId, String tableId, long purgeAfterMs);

  /**
   * Restore a soft deleted table
   *
   * @param databaseId The database ID
   * @param tableId The table ID
   * @param deletedAtMs The timestamp when the table was deleted
   */
  void restoreTable(String databaseId, String tableId, long deletedAtMs);
}
