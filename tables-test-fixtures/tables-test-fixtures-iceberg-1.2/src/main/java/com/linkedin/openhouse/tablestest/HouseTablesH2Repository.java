package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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

  Optional<HouseTable> findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
      String databaseId, String tableId);

  @Override
  default Optional<HouseTable> findById(HouseTablePrimaryKey houseTablePrimaryKey) {
    return this.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
        houseTablePrimaryKey.getDatabaseId(), houseTablePrimaryKey.getTableId());
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
                  houseTable.toBuilder().tableId(toTableId).tableLocation(metadataLocation).build();
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
    }
  }
}
