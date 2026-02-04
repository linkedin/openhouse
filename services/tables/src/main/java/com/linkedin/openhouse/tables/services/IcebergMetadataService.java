package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.model.IcebergMetadata;
import com.linkedin.openhouse.tables.model.IcebergMetadataDiff;
import com.linkedin.openhouse.tables.model.TableData;

/** Service layer for loading Iceberg metadata provided by client. */
public interface IcebergMetadataService {

  /**
   * Get table metadata including full metadata.json and history
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param actingPrincipal The authenticated user principal
   * @return TableMetadata with detailed metadata
   */
  IcebergMetadata getIcebergMetadata(String databaseId, String tableId, String actingPrincipal);

  /**
   * Get metadata diff between a specific snapshot and its immediate predecessor
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param snapshotId The snapshot ID to compare (current)
   * @param actingPrincipal The authenticated user principal
   * @return IcebergMetadataDiff containing current and previous metadata
   */
  IcebergMetadataDiff getMetadataDiff(
      String databaseId, String tableId, Long snapshotId, String actingPrincipal);

  /**
   * Get the first N rows of data from an Iceberg table
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @param limit Maximum number of rows to return
   * @param actingPrincipal The authenticated user principal
   * @return TableData containing rows and metadata
   */
  TableData getTableData(String databaseId, String tableId, int limit, String actingPrincipal);
}
