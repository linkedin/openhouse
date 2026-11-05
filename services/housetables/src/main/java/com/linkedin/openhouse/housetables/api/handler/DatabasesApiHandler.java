package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.api.spec.request.CreateDatabaseRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateDatabasePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;

public interface DatabasesApiHandler {
  ApiResponse<DatabaseMetadata> createDatabase(CreateDatabaseRequestBody request)
      throws DatabaseMetadataException;

  ApiResponse<DatabaseMetadata> getDatabase(String databaseName) throws DatabaseMetadataException;

  ApiResponse<Void> deleteDatabase(String databaseName) throws DatabaseMetadataException;

  ApiResponse<UpdateDatabasePropertiesResponseBody> updateDatabaseProperties(
      String databaseName, UpdateDatabasePropertiesRequestBody request)
      throws DatabaseMetadataException;
}
