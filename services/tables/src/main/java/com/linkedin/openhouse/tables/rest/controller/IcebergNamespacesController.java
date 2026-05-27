package com.linkedin.openhouse.tables.rest.controller;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.rest.adapter.IcebergRestJson;
import com.linkedin.openhouse.tables.rest.adapter.NamespaceUtilRest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
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
import org.springframework.web.bind.annotation.RestController;

/**
 * Iceberg REST Catalog namespace endpoints under {@code /iceberg/v1/{prefix}/namespaces/...}.
 *
 * <p>OpenHouse namespaces are implicit — they spring into existence when their first table is
 * created and stop existing when their last table is dropped. We surface them via {@link
 * DatabasesApiHandler#getAllDatabases()} for listing and existence checks. Creation is a no-op
 * success (we don't pre-create), and deletion is accepted as a no-op success because OpenHouse has
 * no explicit drop-namespace operation.
 *
 * <p>Only single-level namespaces are supported; deeper paths return 400.
 */
@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping("/iceberg/v1/{prefix}")
public class IcebergNamespacesController {

  private final DatabasesApiHandler databasesApiHandler;

  // ----------------------------------------------------------------------------------------------
  // GET /namespaces
  // ----------------------------------------------------------------------------------------------
  @GetMapping(value = "/namespaces", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listNamespaces(@PathVariable("prefix") String prefix)
      throws Exception {
    ApiResponse<GetAllDatabasesResponseBody> resp = databasesApiHandler.getAllDatabases();
    List<GetDatabaseResponseBody> dbs =
        resp.getResponseBody() == null || resp.getResponseBody().getResults() == null
            ? Collections.emptyList()
            : resp.getResponseBody().getResults();

    List<Namespace> namespaces = new ArrayList<>(dbs.size());
    for (GetDatabaseResponseBody db : dbs) {
      namespaces.add(Namespace.of(db.getDatabaseId()));
    }
    ListNamespacesResponse out = ListNamespacesResponse.builder().addAll(namespaces).build();
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(out));
  }

  // ----------------------------------------------------------------------------------------------
  // POST /namespaces — accept, but no-op (OpenHouse namespaces are auto-created on first table)
  // ----------------------------------------------------------------------------------------------
  @PostMapping(
      value = "/namespaces",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createNamespace(
      @PathVariable("prefix") String prefix, @RequestBody String requestBody) throws Exception {
    CreateNamespaceRequest req =
        IcebergRestJson.mapper().readValue(requestBody, CreateNamespaceRequest.class);
    Namespace ns = req.namespace();
    NamespaceUtilRest.requireDepthOne(ns);

    // OpenHouse doesn't pre-create namespaces; we return success and silently drop properties.
    CreateNamespaceResponse out =
        CreateNamespaceResponse.builder()
            .withNamespace(ns)
            .setProperties(Collections.emptyMap())
            .build();
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(out));
  }

  // ----------------------------------------------------------------------------------------------
  // GET /namespaces/{namespace}
  // ----------------------------------------------------------------------------------------------
  @GetMapping(value = "/namespaces/{namespace}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> loadNamespace(
      @PathVariable("prefix") String prefix, @PathVariable("namespace") String namespacePath)
      throws Exception {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    NamespaceUtilRest.requireDepthOne(ns);

    if (!namespaceExists(ns)) {
      throw new NoSuchEntityException("Namespace", ns.toString());
    }

    GetNamespaceResponse out =
        GetNamespaceResponse.builder()
            .withNamespace(ns)
            .setProperties(Collections.emptyMap())
            .build();
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(IcebergRestJson.mapper().writeValueAsString(out));
  }

  // ----------------------------------------------------------------------------------------------
  // HEAD /namespaces/{namespace}
  // ----------------------------------------------------------------------------------------------
  @RequestMapping(value = "/namespaces/{namespace}", method = RequestMethod.HEAD)
  public ResponseEntity<Void> namespaceExistsHead(
      @PathVariable("prefix") String prefix, @PathVariable("namespace") String namespacePath) {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    NamespaceUtilRest.requireDepthOne(ns);

    if (!namespaceExists(ns)) {
      // Let the exception handler render the 404 ErrorResponse for consistency.
      throw new NoSuchEntityException("Namespace", ns.toString());
    }
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  // ----------------------------------------------------------------------------------------------
  // DELETE /namespaces/{namespace} — OpenHouse has no drop-namespace; accept as no-op.
  // ----------------------------------------------------------------------------------------------
  @DeleteMapping(value = "/namespaces/{namespace}")
  public ResponseEntity<Void> dropNamespace(
      @PathVariable("prefix") String prefix, @PathVariable("namespace") String namespacePath) {
    Namespace ns = NamespaceUtilRest.decode(namespacePath);
    NamespaceUtilRest.requireDepthOne(ns);
    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
  }

  // ----------------------------------------------------------------------------------------------
  // helpers
  // ----------------------------------------------------------------------------------------------
  private boolean namespaceExists(Namespace ns) {
    ApiResponse<GetAllDatabasesResponseBody> resp = databasesApiHandler.getAllDatabases();
    if (resp.getResponseBody() == null || resp.getResponseBody().getResults() == null) {
      return false;
    }
    String dbId = ns.level(0);
    for (GetDatabaseResponseBody db : resp.getResponseBody().getResults()) {
      if (dbId.equals(db.getDatabaseId())) {
        return true;
      }
    }
    return false;
  }
}
