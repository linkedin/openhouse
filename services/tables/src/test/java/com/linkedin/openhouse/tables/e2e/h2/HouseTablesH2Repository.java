package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

/**
 * The {@link org.springframework.context.annotation.Bean} injected into /tables e2e tests when
 * communication to the implementation of {@link HouseTableRepository} is not needed. With {@link
 * Primary} annotation, this repository will be the default injection.
 */
@Repository
@Primary
public interface HouseTablesH2Repository extends HouseTableRepository {

  Map<SoftDeletedTablePrimaryKey, HouseTable> softDeletedTables = new HashMap<>();

  /* Default bodies throughout: Spring Data would derive a query from any abstract method name,
   * and none of these predicates is derivable. */

  String ENTITY_TYPE_TABLE = "TABLE";

  String ENTITY_TYPE_VIEW = "VIEW";

  Optional<HouseTable> findByDatabaseIdAndTableId(String databaseId, String tableId);

  /** Untyped, so a typed list can filter before it paginates. */
  List<HouseTable> findByDatabaseId(String databaseId);

  /**
   * Sorting stays in the derived query; a comparator would drop the SQL-side {@link Sort} parts.
   */
  List<HouseTable> findByDatabaseId(String databaseId, Sort sort);

  /** Mirrors the column converter: a pre-discriminator row is a table. */
  static HouseTable hydrateEntityType(HouseTable houseTable) {
    return houseTable.getEntityType() == null
        ? houseTable.toBuilder().entityType(ENTITY_TYPE_TABLE).build()
        : houseTable;
  }

  /** The null arm is load-bearing: legacy rows are tables. */
  static boolean isTableOrLegacy(HouseTable houseTable) {
    return houseTable.getEntityType() == null
        || ENTITY_TYPE_TABLE.equalsIgnoreCase(houseTable.getEntityType());
  }

  static boolean isView(HouseTable houseTable) {
    return ENTITY_TYPE_VIEW.equalsIgnoreCase(houseTable.getEntityType());
  }

  /** Slices already-ordered rows, so filtering in Java cannot make page two arbitrary. */
  static Page<HouseTable> pageOf(List<HouseTable> sortedRows, Pageable pageable) {
    int page = pageable.getPageNumber();
    int size = pageable.getPageSize();
    List<HouseTable> pageContent =
        sortedRows.subList(
            Math.min(page * size, sortedRows.size()),
            Math.min((page + 1) * size, sortedRows.size()));
    return new PageImpl<>(pageContent, pageable, sortedRows.size());
  }

  /** A view at a shared key is absent here, keeping it out of every table path. */
  @Override
  default Optional<HouseTable> findById(HouseTablePrimaryKey houseTablePrimaryKey) {
    return this.findByDatabaseIdAndTableId(
            houseTablePrimaryKey.getDatabaseId(), houseTablePrimaryKey.getTableId())
        .filter(HouseTablesH2Repository::isTableOrLegacy)
        .map(HouseTablesH2Repository::hydrateEntityType);
  }

  /** Filtered before the page is cut, so a view never consumes a slot or inflates a total. */
  @Override
  default List<HouseTable> findAllByDatabaseId(String databaseId) {
    return this.findByDatabaseId(databaseId).stream()
        .filter(HouseTablesH2Repository::isTableOrLegacy)
        .map(HouseTablesH2Repository::hydrateEntityType)
        .collect(Collectors.toList());
  }

  @Override
  default Page<HouseTable> findAllByDatabaseId(String databaseId, Pageable pageable) {
    return pageOf(
        this.findByDatabaseId(databaseId, pageable.getSort()).stream()
            .filter(HouseTablesH2Repository::isTableOrLegacy)
            .map(HouseTablesH2Repository::hydrateEntityType)
            .collect(Collectors.toList()),
        pageable);
  }

  @Override
  default Optional<HouseTable> findEntityById(HouseTablePrimaryKey houseTablePrimaryKey) {
    return this.findByDatabaseIdAndTableId(
            houseTablePrimaryKey.getDatabaseId(), houseTablePrimaryKey.getTableId())
        .map(HouseTablesH2Repository::hydrateEntityType);
  }

  /** Queries the shared key space directly; the table read would find nothing. */
  @Override
  default Optional<HouseTable> findViewById(HouseTablePrimaryKey houseTablePrimaryKey) {
    return this.findByDatabaseIdAndTableId(
            houseTablePrimaryKey.getDatabaseId(), houseTablePrimaryKey.getTableId())
        .filter(HouseTablesH2Repository::isView);
  }

  @Override
  default Page<HouseTable> findAllViewsByDatabaseId(String databaseId, Pageable pageable) {
    return pageOf(
        this.findByDatabaseId(databaseId, pageable.getSort()).stream()
            .filter(HouseTablesH2Repository::isView)
            .collect(Collectors.toList()),
        pageable);
  }

  @Override
  default HouseTable saveView(HouseTable houseTable) {
    // Mirrors House Table stamping the type from the route.
    return this.save(houseTable.toBuilder().entityType(ENTITY_TYPE_VIEW).build());
  }

  @Override
  default boolean deleteViewById(HouseTablePrimaryKey houseTablePrimaryKey) {
    if (!this.findViewById(houseTablePrimaryKey).isPresent()) {
      return false;
    }
    this.deleteById(houseTablePrimaryKey);
    return true;
  }

  @Override
  default void rename(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation) {
    HouseTablePrimaryKey fromKey =
        HouseTablePrimaryKey.builder().databaseId(fromDatabaseId).tableId(fromTableId).build();
    this.findById(fromKey)
        .ifPresent(
            houseTable -> {
              HouseTable renamedTable =
                  houseTable
                      .toBuilder()
                      .databaseId(toDatabaseId)
                      .tableId(toTableId)
                      .tableLocation(metadataLocation)
                      .build();
              this.save(renamedTable);
              this.delete(houseTable);
            });
  }

  @Override
  default void deleteById(HouseTablePrimaryKey houseTablePrimaryKey, boolean isSoftDelete) {
    // For the purpose of testing, move the table to a soft-deleted map instead of deleting it.
    // If HTS is enabled it will write to a different table
    if (this.findById(houseTablePrimaryKey).isPresent()) {
      if (isSoftDelete) {
        SoftDeletedTablePrimaryKey key =
            SoftDeletedTablePrimaryKey.builder()
                .databaseId(houseTablePrimaryKey.getDatabaseId())
                .tableId(houseTablePrimaryKey.getTableId())
                .deletedAtMs(System.currentTimeMillis())
                .build();
        softDeletedTables.put(key, this.findById(houseTablePrimaryKey).get());
      }
      deleteById(houseTablePrimaryKey);
    }
  }

  default Page<HouseTable> searchSoftDeletedTables(
      String databaseId, String tableId, Pageable pageable) {
    List<HouseTable> foundTables = new ArrayList<>();
    for (HouseTable table : softDeletedTables.values()) {
      if (table.getDatabaseId().equals(databaseId)) {
        if (tableId != null && !table.getTableId().equalsIgnoreCase(tableId)) {
          continue; // Filter by tableId if provided
        }
        foundTables.add(table);
      }
    }
    int page = pageable.getPageNumber();
    int size = pageable.getPageSize();
    List<HouseTable> pageContent =
        foundTables.subList(
            Math.min(page * size, foundTables.size()),
            Math.min((page + 1) * size, foundTables.size()));
    int numPages = (int) Math.ceil((double) foundTables.size() / size); // make sure at least 1
    return new PageImpl<>(
        pageContent, PageRequest.of(page, numPages == 0 ? 1 : numPages), foundTables.size());
  }

  default void purgeSoftDeletedTables(String databaseId, String tableId, long purgeAfterMs) {
    // Mock the purge logic on HTS for soft deleted tables
    softDeletedTables
        .entrySet()
        .removeIf(
            entry ->
                entry.getKey().getTableId().equals(tableId)
                    && entry.getKey().getDatabaseId().equals(databaseId)
                    && entry.getValue().getPurgeAfterMs() < purgeAfterMs);
  }

  default void restoreTable(String databaseId, String tableId, long deletedAtMs) {
    SoftDeletedTablePrimaryKey key =
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .deletedAtMs(deletedAtMs)
            .build();

    if (softDeletedTables.containsKey(key)) {
      HouseTable restoredTable = softDeletedTables.remove(key);
      // Restore the table to the main repository
      this.save(restoredTable);
    } else {
      // Throw NoSuchUserTableException when table is not found in soft deleted tables
      throw new com.linkedin.openhouse.common.exception.NoSuchUserTableException(
          databaseId, tableId);
    }
  }
}
