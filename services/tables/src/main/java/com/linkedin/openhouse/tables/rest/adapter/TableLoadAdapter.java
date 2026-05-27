package com.linkedin.openhouse.tables.rest.adapter;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.stereotype.Component;

/**
 * Loads an OpenHouse-backed Iceberg table via {@link OpenHouseInternalCatalog} and wraps it into an
 * Iceberg REST {@link LoadTableResponse}. The metadata-location field of the response is filled
 * from {@link TableMetadata#metadataFileLocation()} by the Iceberg builder.
 */
@Component
@RequiredArgsConstructor
public class TableLoadAdapter {

  private final OpenHouseInternalCatalog openHouseInternalCatalog;

  public LoadTableResponse buildLoadTableResponse(String databaseId, String tableId) {
    TableIdentifier id = TableIdentifier.of(Namespace.of(databaseId), tableId);
    Table table = openHouseInternalCatalog.loadTable(id);
    BaseTable baseTable = (BaseTable) table;
    TableMetadata metadata = baseTable.operations().current();
    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  /** Convenience: also returns the underlying metadata, for callers that need both. */
  public TableMetadata loadMetadata(String databaseId, String tableId) {
    TableIdentifier id = TableIdentifier.of(Namespace.of(databaseId), tableId);
    Table table = openHouseInternalCatalog.loadTable(id);
    BaseTable baseTable = (BaseTable) table;
    return baseTable.operations().current();
  }
}
