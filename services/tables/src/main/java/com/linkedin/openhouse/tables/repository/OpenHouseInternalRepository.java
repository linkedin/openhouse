package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import java.util.List;
import java.util.Optional;
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

  /**
   * Returns a lightweight {@link TableDto} that acts as a catalog-level reference to the table —
   * populated only with the identifiers, tableUUID, and the metadata.json location. Unlike {@link
   * #findById}, this does not parse the table's metadata.json, so it succeeds even when the
   * metadata is corrupted. Intended for paths (e.g. drop) that need only the reference, not the
   * full table state.
   */
  Optional<TableDto> findTableRefById(TableDtoPrimaryKey tableDtoPrimaryKey);

  /**
   * Name occupancy, not table existence: unlike {@link #findById}/{@link #findTableRefById} this
   * sees every pointer row and never parses metadata.json. Empty means no row exists; a null or
   * {@code TABLE} discriminator returns {@code "TABLE"}, and an unrecognized value is returned as
   * stored so an occupied name fails closed.
   *
   * <p>HTS errors must propagate — swallowing them into an empty result would read as "this name is
   * free" and let a CREATE clobber an existing view.
   */
  Optional<String> findOccupyingEntityTypeById(TableDtoPrimaryKey tableDtoPrimaryKey);

  List<TableDtoPrimaryKey> findAllIds();

  Page<TableDtoPrimaryKey> findAllIds(Pageable pageable);

  List<TableDto> searchTables(String databaseId);

  Page<TableDto> searchTables(String databaseId, Pageable pageable);

  /**
   * Paginated search that populates a caller-selected subset of fields on each returned {@link
   * TableDto}. Passing a null or empty {@code fields} list returns identifier-only results (same as
   * the two-arg overload).
   */
  Page<TableDto> searchTables(String databaseId, Pageable pageable, List<String> fields);

  void rename(TableDtoPrimaryKey from, TableDtoPrimaryKey to);

  Page<SoftDeletedTableDto> searchSoftDeletedTables(
      String databaseId, String tableId, Pageable pageable);

  void purgeSoftDeletedTableById(TableDtoPrimaryKey tableDtoPrimaryKey, long purgeAfterMs);

  void restoreTable(SoftDeletedTablePrimaryKey softDeletedTablePrimaryKey);
}
