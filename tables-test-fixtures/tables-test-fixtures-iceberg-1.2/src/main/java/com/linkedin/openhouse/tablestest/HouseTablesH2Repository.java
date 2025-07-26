package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

/**
 * The {@link org.springframework.context.annotation.Bean} injected into /tables e2e tests when
 * communication to the implementation of {@link HouseTableRepository} is not needed. With {@link
 * Primary} annotation, this repository will be the default injection.
 */
@Repository
@Primary
public interface HouseTablesH2Repository extends HouseTableRepository {

  Map<HouseTablePrimaryKey, HouseTable> softDeletedTables = new HashMap<>();

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
        softDeletedTables.put(houseTablePrimaryKey, this.findById(houseTablePrimaryKey).get());
      }
      deleteById(houseTablePrimaryKey);
    }
  }
}
