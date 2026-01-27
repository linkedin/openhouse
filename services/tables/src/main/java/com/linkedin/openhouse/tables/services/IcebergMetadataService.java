package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.model.IcebergMetadata;

/** Service layer for loading Iceberg metadata provided by client. */
public interface IcebergMetadataService {

  /**
   * Get table metadata including full metadata.json and history
   *
   * @param databaseId Database identifier
   * @param tableId Table identifier
   * @return TableMetadata with detailed metadata
   */
  IcebergMetadata getIcebergMetadata(String databaseId, String tableId, String actingPrincipal);
}
