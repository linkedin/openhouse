package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.generated.iceberg.api.CatalogApiApi;
import com.linkedin.openhouse.tables.generated.iceberg.api.ConfigurationApiApi;
import com.linkedin.openhouse.tables.services.TablesService;
import io.swagger.v3.oas.annotations.Hidden;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;

/**
 * Read-only Iceberg REST Catalog surface.
 *
 * <p>Implements the generated {@link CatalogApiApi} and {@link ConfigurationApiApi} interfaces from
 * the upstream Iceberg REST OpenAPI spec. Only config/list/load/exists are overridden; all other
 * endpoints inherit the generated 501 (Not Implemented) default.
 *
 * <p>The {@code /v1/config} endpoint returns a {@code prefix} override so that the Iceberg REST
 * client addresses all subsequent requests via {@code /v1/{prefix}/namespaces/...}, keeping them
 * separate from the existing OpenHouse API routes under {@code /v1/databases/...}.
 *
 * <p>Serialization of Iceberg REST types is handled by {@link IcebergRestHttpMessageConverter},
 * registered in {@link IcebergRestSerdeConfig}.
 */
@Hidden // Exclude from SpringDoc OpenAPI spec to avoid operationId clashes with OpenHouse API
@RestController
public class IcebergRestCatalogController implements CatalogApiApi, ConfigurationApiApi {

  /** Prefix returned by {@code /v1/config} and used in all Iceberg REST routes. */
  public static final String ICEBERG_REST_PREFIX = "iceberg";

  private final OpenHouseInternalCatalog openHouseInternalCatalog;

  private final TablesService tablesService;

  private final TablesApiValidator tablesApiValidator;

  public IcebergRestCatalogController(
      OpenHouseInternalCatalog openHouseInternalCatalog,
      TablesService tablesService,
      TablesApiValidator tablesApiValidator) {
    this.openHouseInternalCatalog = openHouseInternalCatalog;
    this.tablesService = tablesService;
    this.tablesApiValidator = tablesApiValidator;
  }

  /** Resolves the diamond-inherited {@code getRequest()} from both interfaces. */
  @Override
  public Optional<NativeWebRequest> getRequest() {
    return Optional.empty();
  }

  @Override
  public ResponseEntity<ConfigResponse> getConfig(String warehouse) {
    ConfigResponse response =
        ConfigResponse.builder().withOverride("prefix", ICEBERG_REST_PREFIX).build();
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<ListTablesResponse> listTables(
      String prefix, String namespace, String pageToken, Integer pageSize) {
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    tablesApiValidator.validateSearchTables(databaseId);

    List<TableIdentifier> tableIdentifiers =
        tablesService.searchTables(databaseId, extractAuthenticatedUserPrincipal()).stream()
            .map(table -> TableIdentifier.of(icebergNamespace, table.getTableId()))
            .collect(Collectors.toList());
    ListTablesResponse response = ListTablesResponse.builder().addAll(tableIdentifiers).build();
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<LoadTableResponse> loadTable(
      String prefix,
      String namespace,
      String table,
      String xIcebergAccessDelegation,
      String ifNoneMatch,
      String snapshots) {
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
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<Void> tableExists(String prefix, String namespace, String table) {
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
