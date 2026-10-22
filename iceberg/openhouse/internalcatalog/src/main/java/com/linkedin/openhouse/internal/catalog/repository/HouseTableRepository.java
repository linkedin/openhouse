package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

/**
 * Base interface for repository backed by HouseTableService for storing and retrieving {@link
 * HouseTable} object.
 */
@Repository
public interface HouseTableRepository
    extends PagingAndSortingRepository<HouseTable, HouseTablePrimaryKey> {

  List<HouseTable> findAllByDatabaseId(String databaseId);

  /**
   * Delete a table by its primary key with purge option
   *
   * @param houseTablePrimaryKey the primary key of the table
   * @param purge true if table should be deleted permanently, otherwise retain with soft delete
   */
  void deleteById(HouseTablePrimaryKey houseTablePrimaryKey, boolean purge);

  Page<HouseTable> findAllByDatabaseId(String databaseId, Pageable pageable);

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

  /**
   * Any occupant of the key, so a create can classify a collision; advisory, never a precondition.
   */
  Optional<HouseTable> findEntityById(HouseTablePrimaryKey houseTablePrimaryKey);

  /*
   * The unchecked `throws IllegalStateException` on the two typed reads must stay: Spring re-throws
   * an exception the method declares before JPA-translating it, and a Javadoc @throws does not
   * qualify. Declared on the implementation too, so either proxy strategy sees it.
   */

  /** Resolves only VIEW rows; a table at the same key reads as absent. */
  Optional<HouseTable> findViewById(HouseTablePrimaryKey houseTablePrimaryKey)
      throws IllegalStateException;

  /** Lists VIEW rows only; one non-view row fails the page rather than being dropped from it. */
  Page<HouseTable> findAllViewsByDatabaseId(String databaseId, Pageable pageable)
      throws IllegalStateException;

  /** One attempt, never retried: an ambiguous outcome must not become a double write. */
  HouseTable saveView(HouseTable houseTable);

  /** Hard delete, one attempt; false when the key is absent or holds a non-view. */
  boolean deleteViewById(HouseTablePrimaryKey houseTablePrimaryKey);
}
