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
   * Returns whichever entity occupies the key, of any type, so a caller can classify a collision.
   * Advisory only: never a write precondition, because the single House Table compare-and-swap is
   * the sole race arbiter.
   *
   * <p>Deliberately untyped: it has no expected entity type, so it never applies the typed-view
   * contract check. A legacy row arrives already resolved to {@code TABLE}.
   *
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException
   *     the request was rejected as invalid or unauthorized
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableRepositoryStateUnknownException the read could not be completed
   */
  Optional<HouseTable> findEntityById(HouseTablePrimaryKey houseTablePrimaryKey);

  /*
   * The two typed view reads declare `throws IllegalStateException` even though it is unchecked, and
   * that is load-bearing rather than documentation. This repository is a Spring `@Repository`, so
   * under JPA the persistence exception translator would otherwise rewrite the contract violation
   * below into `InvalidDataAccessApiUsageException` and disguise corruption as data-access misuse;
   * `PersistenceExceptionTranslationInterceptor` rethrows an exception the method declares, and a
   * Javadoc `@throws` alone does not qualify. Declared on both the interface and the implementation
   * so either proxy strategy selects a declaring method. It imposes nothing on callers.
   */

  /**
   * Resolves only VIEW rows; a table at the same key reads as absent.
   *
   * @throws IllegalStateException the typed view endpoint returned a present row whose
   *     discriminator is absent or not canonical VIEW, which is a server contract violation and is
   *     never retried
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException
   *     the request was rejected as invalid or unauthorized
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableRepositoryStateUnknownException the read could not be completed
   */
  Optional<HouseTable> findViewById(HouseTablePrimaryKey houseTablePrimaryKey)
      throws IllegalStateException;

  /**
   * House Table filters VIEW before paginating, so no row is read to be discarded.
   *
   * @throws IllegalStateException any row on the page carries an absent or non-canonical VIEW
   *     discriminator; one bad row fails the page, because dropping it would hide corruption and
   *     invalidate the totals
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableRepositoryStateUnknownException the read could not be completed
   */
  Page<HouseTable> findAllViewsByDatabaseId(String databaseId, Pageable pageable)
      throws IllegalStateException;

  /**
   * Exactly one attempt, un-retried: an ambiguous 5xx, 504, or block timeout surfaces as unknown
   * state rather than a blind second write that could double-apply.
   *
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableConcurrentUpdateException the compare-and-swap lost
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException
   *     the request was rejected as invalid or unauthorized
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableRepositoryStateUnknownException the outcome of the single attempt is unknown
   */
  HouseTable saveView(HouseTable houseTable);

  /**
   * Hard delete; views have no soft-delete store. One attempt, un-retried, for the same reason as
   * {@link #saveView(HouseTable)}.
   *
   * @return false when the key is absent or holds a non-view
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException
   *     the request was rejected as invalid or unauthorized
   * @throws com.linkedin.openhouse.internal.catalog.repository.exception
   *     .HouseTableRepositoryStateUnknownException the outcome of the single attempt is unknown
   */
  boolean deleteViewById(HouseTablePrimaryKey houseTablePrimaryKey);
}
