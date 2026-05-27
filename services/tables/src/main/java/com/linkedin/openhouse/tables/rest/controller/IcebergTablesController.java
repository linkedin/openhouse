package com.linkedin.openhouse.tables.rest.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.IcebergSnapshotsApiHandler;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.rest.adapter.CommitAdapter;
import com.linkedin.openhouse.tables.rest.adapter.CreateTableRequestAdapter;
import com.linkedin.openhouse.tables.rest.adapter.IcebergRestJson;
import com.linkedin.openhouse.tables.rest.adapter.NamespaceUtilRest;
import com.linkedin.openhouse.tables.rest.adapter.TableLoadAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Iceberg REST Catalog table endpoints under {@code /iceberg/v1/{prefix}/namespaces/...}.
 *
 * <p>Reads are delegated to {@link TablesApiHandler} for ACL + existence and then to {@link
 * TableLoadAdapter} (backed by {@code OpenHouseInternalCatalog}) for the live Iceberg metadata.
 *
 * <p>Commits replay the inbound {@code MetadataUpdate}s on top of the current metadata, then route
 * the result to either {@link IcebergSnapshotsApiHandler#putIcebergSnapshots} (when snapshots
 * changed) or {@link TablesApiHandler#updateTable} (metadata-only commit) so OpenHouse's normal
 * write path runs.
 *
 * <p>{@link org.apache.iceberg.rest.responses.LoadTableResponse} is used for both create and commit
 * responses — Iceberg's commit response is structurally identical and {@code CommitTableResponse}
 * does not exist in iceberg-core 1.5.2.
 */
@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping("/iceberg/v1/{prefix}")
public class IcebergTablesController {

  private final TablesApiHandler tablesApiHandler;
  private final IcebergSnapshotsApiHandler icebergSnapshotsApiHandler;
  private final CommitAdapter commitAdapter;
  private final CreateTableRequestAdapter createTableRequestAdapter;
  private final TableLoadAdapter tableLoadAdapter;

  // ----------------------------------------------------------------------------------------------
  // GET /namespaces/{namespace}/tables
  // ----------------------------------------------------------------------------------------------
  @GetMapping(value = "/namespaces/{namespace}/tables", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listTables(
      @PathVariable("prefix") String prefix, @PathVariable("namespace") String namespacePath)
      throws Exception {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);

    ApiResponse<GetAllTablesResponseBody> resp = tablesApiHandler.searchTables(databaseId);
    List<GetTableResponseBody> tables =
        resp.getResponseBody() == null || resp.getResponseBody().getResults() == null
            ? Collections.emptyList()
            : resp.getResponseBody().getResults();

    ListTablesResponse.Builder builder = ListTablesResponse.builder();
    for (GetTableResponseBody t : tables) {
      builder.add(
          org.apache.iceberg.catalog.TableIdentifier.of(
              Namespace.of(t.getDatabaseId()), t.getTableId()));
    }
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(builder.build()));
  }

  // ----------------------------------------------------------------------------------------------
  // GET /namespaces/{namespace}/tables/{table}
  // ----------------------------------------------------------------------------------------------
  @GetMapping(
      value = "/namespaces/{namespace}/tables/{table}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> loadTable(
      @PathVariable("prefix") String prefix,
      @PathVariable("namespace") String namespacePath,
      @PathVariable("table") String tableId)
      throws Exception {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);
    String principal = extractAuthenticatedUserPrincipal();

    // existence + ACL check via OpenHouse handler; throws NoSuchEntityException -> 404
    tablesApiHandler.getTable(databaseId, tableId, principal);

    LoadTableResponse response = tableLoadAdapter.buildLoadTableResponse(databaseId, tableId);
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(response));
  }

  // ----------------------------------------------------------------------------------------------
  // HEAD /namespaces/{namespace}/tables/{table}
  // ----------------------------------------------------------------------------------------------
  @RequestMapping(value = "/namespaces/{namespace}/tables/{table}", method = RequestMethod.HEAD)
  public ResponseEntity<Void> tableExistsHead(
      @PathVariable("prefix") String prefix,
      @PathVariable("namespace") String namespacePath,
      @PathVariable("table") String tableId) {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);
    String principal = extractAuthenticatedUserPrincipal();

    // Throws NoSuchEntityException -> 404 via exception handler.
    tablesApiHandler.getTable(databaseId, tableId, principal);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  // ----------------------------------------------------------------------------------------------
  // POST /namespaces/{namespace}/tables — create
  // ----------------------------------------------------------------------------------------------
  @PostMapping(
      value = "/namespaces/{namespace}/tables",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createTable(
      @PathVariable("prefix") String prefix,
      @PathVariable("namespace") String namespacePath,
      @RequestBody String requestBody)
      throws Exception {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);
    String principal = extractAuthenticatedUserPrincipal();

    CreateTableRequest req =
        IcebergRestJson.mapper().readValue(requestBody, CreateTableRequest.class);
    if (req.stageCreate()) {
      // v0 limitation: staged-create (CTAS) requires separate stage + commit handling.
      return errorResponse(
          HttpStatus.NOT_IMPLEMENTED,
          "BadRequestException",
          "stage-create (CTAS) is not supported by the OpenHouse Iceberg REST adapter v0");
    }

    CreateUpdateTableRequestBody body = createTableRequestAdapter.buildFromCreate(ns, req);
    tablesApiHandler.createTable(databaseId, body, principal);

    // Re-load the freshly-created table to produce a LoadTableResponse with current metadata.
    LoadTableResponse response = tableLoadAdapter.buildLoadTableResponse(databaseId, req.name());
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(response));
  }

  // ----------------------------------------------------------------------------------------------
  // POST /namespaces/{namespace}/tables/{table} — commit (updateTable)
  // ----------------------------------------------------------------------------------------------
  @PostMapping(
      value = "/namespaces/{namespace}/tables/{table}",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> commitTable(
      @PathVariable("prefix") String prefix,
      @PathVariable("namespace") String namespacePath,
      @PathVariable("table") String tableId,
      @RequestBody String requestBody)
      throws Exception {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);
    String principal = extractAuthenticatedUserPrincipal();

    UpdateTableRequest req =
        IcebergRestJson.mapper().readValue(requestBody, UpdateTableRequest.class);

    // 1. Load current metadata
    TableMetadata currentMetadata = tableLoadAdapter.loadMetadata(databaseId, tableId);

    // 2. Validate every requirement against current metadata (throws CommitFailedException -> 409)
    if (req.requirements() != null) {
      for (UpdateRequirement requirement : req.requirements()) {
        requirement.validate(currentMetadata);
      }
    }

    // 3. Replay updates to produce post-commit metadata
    TableMetadata.Builder mdBuilder = TableMetadata.buildFrom(currentMetadata);
    if (req.updates() != null) {
      for (MetadataUpdate u : req.updates()) {
        u.applyTo(mdBuilder);
      }
    }
    TableMetadata newMetadata = mdBuilder.build();

    // 4. Build the OpenHouse request body
    String baseTableVersion = currentMetadata.metadataFileLocation();
    CreateUpdateTableRequestBody body =
        commitAdapter.buildCreateUpdateBody(ns, tableId, newMetadata, baseTableVersion);

    // 5. Discriminate snapshot-changing vs metadata-only commits
    boolean snapshotsChanged =
        !safeEquals(currentMetadata.snapshots(), newMetadata.snapshots())
            || !safeEquals(currentMetadata.refs(), newMetadata.refs());

    if (snapshotsChanged) {
      List<String> jsonSnapshots = new ArrayList<>();
      if (newMetadata.snapshots() != null) {
        for (Snapshot s : newMetadata.snapshots()) {
          jsonSnapshots.add(SnapshotParser.toJson(s));
        }
      }
      Map<String, String> jsonRefs = new LinkedHashMap<>();
      if (newMetadata.refs() != null) {
        for (Map.Entry<String, SnapshotRef> e : newMetadata.refs().entrySet()) {
          jsonRefs.put(e.getKey(), SnapshotRefParser.toJson(e.getValue()));
        }
      }
      IcebergSnapshotsRequestBody snapBody =
          IcebergSnapshotsRequestBody.builder()
              .baseTableVersion(baseTableVersion)
              .jsonSnapshots(jsonSnapshots)
              .snapshotRefs(jsonRefs)
              .createUpdateTableRequestBody(body)
              .build();
      icebergSnapshotsApiHandler.putIcebergSnapshots(databaseId, tableId, snapBody, principal);
    } else {
      tablesApiHandler.updateTable(databaseId, tableId, body, principal);
    }

    // 6. Re-load to produce the committed LoadTableResponse
    LoadTableResponse response = tableLoadAdapter.buildLoadTableResponse(databaseId, tableId);
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(response));
  }

  // ----------------------------------------------------------------------------------------------
  // DELETE /namespaces/{namespace}/tables/{table} — drop
  // ----------------------------------------------------------------------------------------------
  @DeleteMapping(value = "/namespaces/{namespace}/tables/{table}")
  public ResponseEntity<Void> dropTable(
      @PathVariable("prefix") String prefix,
      @PathVariable("namespace") String namespacePath,
      @PathVariable("table") String tableId,
      @RequestParam(value = "purgeRequested", required = false, defaultValue = "false")
          boolean purgeRequested) {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    String databaseId = NamespaceUtilRest.singleLevelDb(ns);
    String principal = extractAuthenticatedUserPrincipal();

    // v0: ignore purgeRequested — OpenHouse soft-deletes by default.
    tablesApiHandler.deleteTable(databaseId, tableId, principal);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  // ----------------------------------------------------------------------------------------------
  // helpers
  // ----------------------------------------------------------------------------------------------

  /** Null-safe equality wrapper for {@code List#equals} / {@code Map#equals}. */
  private static boolean safeEquals(Object a, Object b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    return a.equals(b);
  }

  /** Render a minimal Iceberg ErrorResponse JSON inline (used for special-case 501 below). */
  private ResponseEntity<String> errorResponse(HttpStatus status, String type, String message)
      throws Exception {
    org.apache.iceberg.rest.responses.ErrorResponse err =
        org.apache.iceberg.rest.responses.ErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(status.value())
            .build();
    String json = org.apache.iceberg.rest.responses.ErrorResponseParser.toJson(err);
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(json);
  }

  // Avoid an unused import warning if CommitFailedException is referenced only in javadoc.
  @SuppressWarnings("unused")
  private static Class<?> unusedCommitFailed() {
    return CommitFailedException.class;
  }
}
