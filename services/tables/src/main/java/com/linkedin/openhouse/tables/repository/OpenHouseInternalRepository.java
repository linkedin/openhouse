package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

/**
 * Base interface for repository backed by OpenHouseInternalCatalog for storing and retrieving
 * {@link TableDto} object.
 */
@Repository
public interface OpenHouseInternalRepository
    extends PagingAndSortingRepository<TableDto, TableDtoPrimaryKey> {
  List<TableDtoPrimaryKey> findAllIds();

  Page<TableDtoPrimaryKey> findAllIds(Pageable pageable);

  List<TableDto> searchTables(String databaseId);

  Page<TableDto> searchTables(String databaseId, Pageable pageable);

  void rename(TableDtoPrimaryKey from, TableDtoPrimaryKey to);

  Page<SoftDeletedTableDto> searchSoftDeletedTables(
      String databaseId, String tableId, Pageable pageable);

  void purgeSoftDeletedTableById(TableDtoPrimaryKey tableDtoPrimaryKey, long purgeAfterMs);

  void restoreTable(SoftDeletedTablePrimaryKey softDeletedTablePrimaryKey);

  /**
   * Swap the Iceberg metadata.location for a table to {@code newUri}. This writes a new
   * metadata.json at the new path, redirecting all subsequent data and metadata writes there.
   * Existing manifest entries remain valid at their original locations.
   *
   * @param databaseId database containing the table
   * @param tableId the table to update
   * @param newUri base URI of the new storage location (the new Iceberg metadata.location)
   */
  void swapStorageLocation(String databaseId, String tableId, String newUri);
}
