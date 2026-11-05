package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateDatabasePropertiesResponseBody;
import java.util.Map;
import java.util.Set;

public interface DatabasesService {
  DatabaseMetadata createDatabase(String databaseName, Map<String, String> properties)
      throws DatabaseMetadataException;

  DatabaseMetadata getDatabase(String databaseName) throws DatabaseMetadataException;

  void deleteDatabase(String databaseName) throws DatabaseMetadataException;

  UpdateDatabasePropertiesResponseBody updateDatabaseProperties(
      String databaseName, Map<String, String> updates, Set<String> removals)
      throws DatabaseMetadataException;
}
