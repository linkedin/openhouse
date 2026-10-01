package com.linkedin.openhouse.tables.controller;

import com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler;
import com.linkedin.openhouse.tables.generated.iceberg.api.CatalogApiApi;
import com.linkedin.openhouse.tables.generated.iceberg.api.ConfigurationApiApi;
import com.linkedin.openhouse.tables.generated.iceberg.model.CatalogConfig;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.Optional;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;

/**
 * Thin Spring MVC adapter for the generated read-only Iceberg REST contract.
 *
 * <p>Protocol translation and orchestration live in {@link IcebergRestApiHandler}; existing
 * OpenHouse handlers and services remain the source of business behavior.
 */
@Hidden
@RestController
@ConditionalOnProperty(value = "cluster.tables.iceberg-rest.enabled", havingValue = "true")
public class IcebergRestCatalogController implements CatalogApiApi, ConfigurationApiApi {

  private final IcebergRestApiHandler icebergRestApiHandler;

  public IcebergRestCatalogController(IcebergRestApiHandler icebergRestApiHandler) {
    this.icebergRestApiHandler = icebergRestApiHandler;
  }

  @Override
  public Optional<NativeWebRequest> getRequest() {
    return Optional.empty();
  }

  @Override
  public ResponseEntity<CatalogConfig> getConfig(String warehouse) {
    return ResponseEntity.ok(icebergRestApiHandler.getConfig(warehouse));
  }

  @Override
  public ResponseEntity<ListTablesResponse> listTables(
      String prefix, String namespace, String pageToken, Integer pageSize) {
    return ResponseEntity.ok(
        icebergRestApiHandler.listTables(prefix, namespace, pageToken, pageSize));
  }

  @Override
  public ResponseEntity<LoadTableResponse> loadTable(
      String prefix,
      String namespace,
      String table,
      String xIcebergAccessDelegation,
      String ifNoneMatch,
      String snapshots,
      String referencedBy) {
    return ResponseEntity.ok(
        icebergRestApiHandler.loadTable(
            prefix,
            namespace,
            table,
            xIcebergAccessDelegation,
            ifNoneMatch,
            snapshots,
            referencedBy));
  }

  @Override
  public ResponseEntity<Void> tableExists(String prefix, String namespace, String table) {
    icebergRestApiHandler.tableExists(prefix, namespace, table);
    return ResponseEntity.noContent().build();
  }
}
