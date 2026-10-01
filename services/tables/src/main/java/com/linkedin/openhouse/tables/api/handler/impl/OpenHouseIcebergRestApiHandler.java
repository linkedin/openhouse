package com.linkedin.openhouse.tables.api.handler.impl;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.generated.iceberg.IcebergRestOpenHouseSupport;
import com.linkedin.openhouse.tables.generated.iceberg.model.CatalogConfig;
import com.linkedin.openhouse.tables.generated.iceberg.model.ListTablesResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

/** Default Iceberg REST adapter backed by existing OpenHouse API handlers and catalog behavior. */
@Component
@ConditionalOnProperty(value = "cluster.tables.iceberg-rest.enabled", havingValue = "true")
public class OpenHouseIcebergRestApiHandler implements IcebergRestApiHandler {

  static final int DEFAULT_PAGE_SIZE = 100;
  static final int MAX_PAGE_SIZE = 1000;
  private static final String PAGE_TOKEN_VERSION = "v1";

  private final TablesApiHandler tablesApiHandler;
  private final OpenHouseInternalCatalog openHouseInternalCatalog;

  public OpenHouseIcebergRestApiHandler(
      TablesApiHandler tablesApiHandler, OpenHouseInternalCatalog openHouseInternalCatalog) {
    this.tablesApiHandler = tablesApiHandler;
    this.openHouseInternalCatalog = openHouseInternalCatalog;
  }

  @Override
  public CatalogConfig getConfig(String warehouse) {
    return new CatalogConfig(
            Collections.singletonMap("prefix", ICEBERG_REST_PREFIX), Collections.emptyMap())
        .endpoints(IcebergRestOpenHouseSupport.SUPPORTED_ENDPOINTS);
  }

  @Override
  public ListTablesResponse listTables(
      String prefix, String namespace, String pageToken, Integer pageSize) {
    validatePrefix(prefix);
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    PageCursor cursor = decodePageToken(pageToken, pageSize);
    ApiResponse<GetAllTablesResponseBody> response =
        tablesApiHandler.searchTables(
            icebergNamespace.level(0), cursor.getPage(), cursor.getPageSize(), "tableId");
    Page<GetTableResponseBody> page = response.getResponseBody().getPageResults();
    LinkedHashSet<TableIdentifier> identifiers =
        page.getContent().stream()
            .map(table -> TableIdentifier.of(icebergNamespace, table.getTableId()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    String nextPageToken =
        page.hasNext() ? encodePageToken(cursor.getPage() + 1, cursor.getPageSize()) : null;
    return new ListTablesResponse().identifiers(identifiers).nextPageToken(nextPageToken);
  }

  @Override
  public LoadTableResponse loadTable(
      String prefix,
      String namespace,
      String table,
      String accessDelegation,
      String ifNoneMatch,
      String snapshots,
      String referencedBy) {
    validatePrefix(prefix);
    if (snapshots != null && !"all".equals(snapshots)) {
      throw new UnsupportedOperationException(
          "The snapshots=refs projection is not supported by this catalog");
    }
    // Iceberg 1.11 loadTable may send referenced-by for view-load chains; Phase 1 ignores it.

    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    try {
      tablesApiHandler.getTable(databaseId, table, extractAuthenticatedUserPrincipal());
    } catch (NoSuchUserTableException e) {
      throw new NoSuchTableException("Table does not exist: %s.%s", databaseId, table);
    }

    return CatalogHandlers.loadTable(
        openHouseInternalCatalog, TableIdentifier.of(icebergNamespace, table));
  }

  @Override
  public void tableExists(String prefix, String namespace, String table) {
    validatePrefix(prefix);
    Namespace icebergNamespace = decodeSingleLevelNamespace(namespace);
    String databaseId = icebergNamespace.level(0);
    try {
      tablesApiHandler.getTable(databaseId, table, extractAuthenticatedUserPrincipal());
    } catch (NoSuchUserTableException e) {
      throw new NoSuchTableException("Table does not exist: %s.%s", databaseId, table);
    }
  }

  private static void validatePrefix(String prefix) {
    if (!ICEBERG_REST_PREFIX.equals(prefix)) {
      throw new IllegalArgumentException("Unsupported Iceberg REST prefix");
    }
  }

  private static Namespace decodeSingleLevelNamespace(String encodedNamespace) {
    Namespace namespace = RESTUtil.decodeNamespace(encodedNamespace);
    if (namespace.isEmpty() || namespace.levels().length != 1) {
      throw new NoSuchNamespaceException("Only single-level namespaces are supported");
    }
    return namespace;
  }

  private static PageCursor decodePageToken(String pageToken, Integer requestedPageSize) {
    if (pageToken == null) {
      return new PageCursor(0, validatePageSize(requestedPageSize));
    }

    try {
      String decoded = new String(Base64.getUrlDecoder().decode(pageToken), StandardCharsets.UTF_8);
      String[] parts = decoded.split(":", -1);
      if (parts.length != 3 || !PAGE_TOKEN_VERSION.equals(parts[0])) {
        throw new IllegalArgumentException("Invalid Iceberg REST page token");
      }
      int page = Integer.parseInt(parts[1]);
      int pageSize = validatePageSize(Integer.parseInt(parts[2]));
      if (page < 1 || (requestedPageSize != null && requestedPageSize != pageSize)) {
        throw new IllegalArgumentException("Invalid Iceberg REST page token");
      }
      return new PageCursor(page, pageSize);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Iceberg REST page token", e);
    }
  }

  private static int validatePageSize(Integer requestedPageSize) {
    int pageSize = requestedPageSize == null ? DEFAULT_PAGE_SIZE : requestedPageSize;
    if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException(
          String.format("page-size must be between 1 and %s", MAX_PAGE_SIZE));
    }
    return pageSize;
  }

  private static String encodePageToken(int page, int pageSize) {
    String value = String.format("%s:%s:%s", PAGE_TOKEN_VERSION, page, pageSize);
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  @lombok.Value
  private static class PageCursor {
    int page;
    int pageSize;
  }
}
