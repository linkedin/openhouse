package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetMetadataDiffResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableDataResponseBody;

/**
 * Interface layer between REST and Iceberg Snapshots backend. The implementation is injected into
 * the Service Controller.
 */
public interface IcebergMetadataApiHandler {
  /**
   * Get table metadata including full metadata.json and history
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param actingPrincipal The authenticated user principal
   * @return GetTableMetadataResponseBody with detailed metadata
   */
  ApiResponse<GetIcebergMetadataResponseBody> getIcebergMetadata(
      String databaseId, String tableId, String actingPrincipal);

  /**
   * Get metadata diff between a specific metadata file and its immediate predecessor
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param metadataFile The metadata file location to compare
   * @param actingPrincipal The authenticated user principal
   * @return GetMetadataDiffResponseBody containing current and previous metadata
   */
  ApiResponse<GetMetadataDiffResponseBody> getMetadataDiff(
      String databaseId, String tableId, String metadataFile, String actingPrincipal);

  /**
   * Get the first N rows of data from an Iceberg table
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param limit Maximum number of rows to return
   * @param actingPrincipal The authenticated user principal
   * @return GetTableDataResponseBody containing rows and metadata
   */
  ApiResponse<GetTableDataResponseBody> getTableData(
      String databaseId, String tableId, int limit, String actingPrincipal);
}
