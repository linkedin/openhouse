package com.linkedin.openhouse.tables.e2e.h2;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
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
}
