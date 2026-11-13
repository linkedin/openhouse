package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.internal.catalog.model.NamespaceMetadata;
import com.linkedin.openhouse.internal.catalog.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.tables.api.spec.namespace.CreateNamespaceRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceErrorResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.NamespaceResponse;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesRequest;
import com.linkedin.openhouse.tables.api.spec.namespace.UpdateNamespacePropertiesResponse;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.services.NamespacesService;
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

/** The prefix identifies the client catalog; namespaces contain exactly one name. */
@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/v1/{prefix}/namespaces", produces = "application/json")
@ApiResponses({
  @ApiResponse(
      responseCode = "400",
      description = "Invalid request, or a nonempty DROP (NamespaceNotEmptyException).",
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
      description =
          "Duplicate creation (CREATE only), or confirmed version conflict (OpenHouse extension: CommitFailedException).",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "422",
      description = "Invalid property update",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "500",
      description =
          "Confirmed service failure, or uncertain mutation outcome (CommitStateUnknownException).",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "503",
      description = "Dependency unavailable",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
})
public class IcebergNamespacesController {
  private final NamespacesService namespacesService;

  @Operation(tags = "Namespace", summary = "Create a namespace")
  @ApiResponse(responseCode = "200", description = "Namespace created")
  @PostMapping
  @Secured(Privileges.Privilege.SYSTEM_ADMIN)
  public NamespaceResponse createNamespace(
      @PathVariable String prefix, @Valid @RequestBody CreateNamespaceRequest request)
      throws DomainException {
    NamespaceMetadata result =
        namespacesService.createNamespace(
            request.getNamespace(), request.getProperties(), extractAuthenticatedUserPrincipal());
    return new NamespaceResponse(
        Collections.singletonList(result.getNamespaceName()), result.getProperties());
  }

  @Operation(tags = "Namespace", summary = "Load namespace metadata")
  @ApiResponse(responseCode = "200", description = "Namespace metadata")
  @GetMapping("/{namespace}")
  @Secured(Privileges.Privilege.GET_TABLE_METADATA)
  public NamespaceResponse loadNamespaceMetadata(
      @PathVariable String prefix, @PathVariable String namespace) throws DomainException {
    NamespaceMetadata result =
        namespacesService.loadNamespaceMetadata(namespace, extractAuthenticatedUserPrincipal());
    return new NamespaceResponse(
        Collections.singletonList(result.getNamespaceName()), result.getProperties());
  }

  @Operation(tags = "Namespace", summary = "Update namespace properties")
  @ApiResponse(responseCode = "200", description = "Properties updated")
  @PostMapping("/{namespace}/properties")
  @Secured(Privileges.Privilege.SYSTEM_ADMIN)
  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      @PathVariable String prefix,
      @PathVariable String namespace,
      @Valid @RequestBody UpdateNamespacePropertiesRequest request)
      throws DomainException {
    NamespacePropertyUpdate result =
        namespacesService.updateNamespaceProperties(
            namespace,
            request.getUpdates(),
            request.getRemovals(),
            extractAuthenticatedUserPrincipal());
    return new UpdateNamespacePropertiesResponse(
        result.getUpdated(), result.getRemoved(), result.getMissing());
  }

  @Operation(tags = "Namespace", summary = "Drop an empty namespace")
  @ApiResponse(responseCode = "204", description = "Namespace dropped")
  @ApiResponse(
      responseCode = "400",
      description = "Invalid request or nonempty namespace (NamespaceNotEmptyException)",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
  @DeleteMapping("/{namespace}")
  @Secured(Privileges.Privilege.SYSTEM_ADMIN)
  public ResponseEntity<Void> dropNamespace(
      @PathVariable String prefix, @PathVariable String namespace) throws DomainException {
    namespacesService.dropNamespace(namespace, extractAuthenticatedUserPrincipal());
    return ResponseEntity.noContent().build();
  }
}
