package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.DatabaseMetadata;
import com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface HouseDatabaseRepository {
  DatabaseMetadata create(String databaseName, Map<String, String> properties)
      throws HouseDatabaseException;

  Optional<DatabaseMetadata> findByName(String databaseName) throws HouseDatabaseException;

  void delete(String databaseName) throws HouseDatabaseException;

  DatabasePropertyUpdate updateProperties(
      String databaseName, Map<String, String> updates, Set<String> removals)
      throws HouseDatabaseException;

  boolean containsEntities(String databaseName) throws HouseDatabaseException;
}
