package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.internal.catalog.model.DatabaseMetadata;
import com.linkedin.openhouse.internal.catalog.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseDatabaseException;
import com.linkedin.openhouse.tables.api.spec.namespace.CreateNamespaceRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesResponse;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.NamespaceException;
import com.linkedin.openhouse.tables.services.IcebergNamespacesService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.Collections;
import javax.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** The prefix identifies the client catalog; the namespace identifies one logical database. */
@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/v1/{prefix}/namespaces", produces = "application/json")
@ApiResponses({
  @ApiResponse(
      responseCode = "400",
      description = "Invalid request",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "401",
      description = "Authentication required",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "403",
      description = "Access denied",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "404",
      description = "Namespace absent",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "409",
      description = "Namespace conflict",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "422",
      description = "Invalid property update",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "500",
      description = "Service failure",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "503",
      description = "Dependency unavailable",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
})
public class IcebergNamespacesController {
  private final IcebergNamespacesService namespacesService;

  @Operation(tags = "Namespace", summary = "Create a namespace")
  @ApiResponse(responseCode = "200", description = "Namespace created")
  @PostMapping
  @Secured(Privileges.Privilege.SYSTEM_ADMIN)
  public NamespaceResponse createNamespace(
      @PathVariable String prefix, @Valid @RequestBody CreateNamespaceRequest request)
      throws NamespaceException, HouseDatabaseException {
    DatabaseMetadata result =
        namespacesService.create(
            request.getNamespace(), request.getProperties(), extractAuthenticatedUserPrincipal());
    return new NamespaceResponse(
        Collections.singletonList(result.getDatabaseName()), result.getProperties());
  }

  @Operation(tags = "Namespace", summary = "Load namespace metadata")
  @ApiResponse(responseCode = "200", description = "Namespace metadata")
  @GetMapping("/{namespace}")
  @Secured(Privileges.Privilege.GET_TABLE_METADATA)
  public NamespaceResponse loadNamespaceMetadata(
      @PathVariable String prefix, @PathVariable String namespace)
      throws NamespaceException, HouseDatabaseException {
    DatabaseMetadata result =
        namespacesService.load(namespace, extractAuthenticatedUserPrincipal());
    return new NamespaceResponse(
        Collections.singletonList(result.getDatabaseName()), result.getProperties());
  }

  @Operation(tags = "Namespace", summary = "Update namespace properties")
  @ApiResponse(responseCode = "200", description = "Properties updated")
  @PostMapping("/{namespace}/properties")
  @Secured(Privileges.Privilege.SYSTEM_ADMIN)
  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      @PathVariable String prefix,
      @PathVariable String namespace,
      @Valid @RequestBody UpdateNamespacePropertiesRequest request)
      throws NamespaceException, HouseDatabaseException {
    DatabasePropertyUpdate result =
        namespacesService.update(
            namespace,
            request.getUpdates(),
            request.getRemovals(),
            extractAuthenticatedUserPrincipal());
    return new UpdateNamespacePropertiesResponse(
        result.getUpdated(), result.getRemoved(), result.getMissing());
  }

  @Operation(tags = "Namespace", summary = "Drop an empty namespace")
  @ApiResponse(responseCode = "204", description = "Namespace dropped")
  @DeleteMapping("/{namespace}")
  @Secured(Privileges.Privilege.DELETE_TABLE)
  public ResponseEntity<Void> dropNamespace(
      @PathVariable String prefix, @PathVariable String namespace)
      throws NamespaceException, HouseDatabaseException {
    namespacesService.drop(namespace, extractAuthenticatedUserPrincipal());
    return ResponseEntity.noContent().build();
  }
}
