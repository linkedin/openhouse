package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.tables.generated.iceberg.model.CatalogConfig;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/** Protocol adapter between the generated Iceberg REST API and existing OpenHouse behavior. */
public interface IcebergRestApiHandler {

  String ICEBERG_REST_PREFIX = "iceberg";

  CatalogConfig getConfig(String warehouse);

  ListTablesResponse listTables(
      String prefix, String namespace, String pageToken, Integer pageSize);

  LoadTableResponse loadTable(
      String prefix,
      String namespace,
      String table,
      String accessDelegation,
      String ifNoneMatch,
      String snapshots,
      String referencedBy);

  void tableExists(String prefix, String namespace, String table);
}
