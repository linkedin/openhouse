package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import java.util.List;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * Base interface for repository backed by OpenHouseInternalCatalog for storing and retrieving
 * {@link TableDto} object.
 */
@Repository
public interface OpenHouseInternalRepository extends CrudRepository<TableDto, TableDtoPrimaryKey> {
  List<TableDtoPrimaryKey> findAllIds();

  List<TableDto> searchTables(String databaseId);

  void rename(TableDtoPrimaryKey from, TableDtoPrimaryKey to);
}
