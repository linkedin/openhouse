package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.CrudRepository;
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
}
