package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import java.util.List;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * Base interface for repository backed by HouseTableService for storing and retrieving {@link
 * HouseTable} object.
 */
@Repository
public interface HouseTableRepository extends CrudRepository<HouseTable, HouseTablePrimaryKey> {

  List<HouseTable> findAllByDatabaseId(String databaseId);

  void rename(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String metadataLocation);
}
