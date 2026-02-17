package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.generated.iceberg.api.IcebergReadOnlyApi;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.services.TablesService;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only Iceberg REST Catalog surface.
 *
 * <p>First iteration intentionally supports only config/list/load paths required for read
 * operations.
 */
@RestController
public class IcebergRestCatalogController implements IcebergReadOnlyApi {

  @Autowired private OpenHouseInternalCatalog openHouseInternalCatalog;

  @Autowired private TablesService tablesService;

  @Autowired private TablesApiValidator tablesApiValidator;

  @Override
  public ResponseEntity<String> getConfig(String warehouse) {
    ConfigResponse response = ConfigResponse.builder().build();
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestSerde.toJson(response));
  }

  @Override
  public ResponseEntity<String> listTables(String namespace) {
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    tablesApiValidator.validateSearchTables(databaseId);

    ListTablesResponse response =
        CatalogHandlers.listTables(openHouseInternalCatalog, icebergNamespace);
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestSerde.toJson(response));
  }

  @Override
  public ResponseEntity<String> loadTable(
      String namespace, String table, String snapshots) {
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    tablesApiValidator.validateGetTable(databaseId, table);

    // Reuse the existing table-read authorization and lock visibility checks.
    try {
      tablesService.getTable(databaseId, table, extractAuthenticatedUserPrincipal());
    } catch (NoSuchUserTableException e) {
      throw new NoSuchTableException("Table does not exist: %s.%s", databaseId, table);
    }

    LoadTableResponse response =
        CatalogHandlers.loadTable(
            openHouseInternalCatalog, TableIdentifier.of(icebergNamespace, table));
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestSerde.toJson(response));
  }

  @Override
  public ResponseEntity<Void> headTable(String namespace, String table) {
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    tablesApiValidator.validateGetTable(databaseId, table);

    try {
      tablesService.getTable(databaseId, table, extractAuthenticatedUserPrincipal());
    } catch (NoSuchUserTableException e) {
      throw new NoSuchTableException("Table does not exist: %s.%s", databaseId, table);
    }

    return ResponseEntity.noContent().build();
  }

  private Namespace decodeSingleLevelNamespace(String encodedNamespace) {
    Namespace namespace = RESTUtil.decodeNamespace(encodedNamespace);
    if (namespace.isEmpty() || namespace.levels().length != 1) {
      throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
    }

    return namespace;
  }
}
