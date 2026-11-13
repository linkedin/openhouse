package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.common.exception.checked.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.checked.AuthenticationRequiredException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.MutationUncertainException;
import com.linkedin.openhouse.common.exception.checked.NamespaceException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.common.exception.checked.PermissionDeniedException;
import com.linkedin.openhouse.common.exception.checked.ServiceFailureException;
import com.linkedin.openhouse.common.exception.checked.UnavailableException;
import com.linkedin.openhouse.common.exception.checked.VersionConflictException;
import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceErrorResponse;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/hts/namespaces", produces = "application/json")
@ApiResponses({
  @ApiResponse(
      responseCode = "400",
      description = "Invalid namespace request.",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "503",
      description = "Namespace metadata is unavailable for this backend.",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
  @ApiResponse(
      responseCode = "500",
      description = "Confirmed service failure, or an uncertain mutation outcome.",
      content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
})
public class NamespacesController {
  private final NamespacesApiHandler namespacesApiHandler;

  public NamespacesController(NamespacesApiHandler namespacesApiHandler) {
    this.namespacesApiHandler = namespacesApiHandler;
  }

  @Operation(
      operationId = "createNamespace",
      tags = "Namespace",
      summary = "Create namespace metadata.")
  @ApiResponses({
    @ApiResponse(responseCode = "201", description = "Namespace metadata created."),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace name already exists, or a confirmed version conflict.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
  })
  @PostMapping(consumes = "application/json")
  public ResponseEntity<NamespaceMetadataResponse> createNamespace(
      @RequestBody CreateNamespaceRequestBody request) throws NamespaceException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<NamespaceMetadataResponse> response =
        namespacesApiHandler.createNamespace(request);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(operationId = "getNamespace", tags = "Namespace", summary = "Get namespace metadata.")
  @ApiResponses({
    @ApiResponse(responseCode = "200", description = "Namespace metadata loaded."),
    @ApiResponse(
        responseCode = "404",
        description = "Namespace metadata was not found.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
  })
  @GetMapping("/{namespaceName}")
  public ResponseEntity<NamespaceMetadataResponse> getNamespace(
      @PathVariable("namespaceName") String namespaceName) throws NamespaceException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<NamespaceMetadataResponse> response =
        namespacesApiHandler.getNamespace(namespaceName);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(
      operationId = "deleteNamespace",
      tags = "Namespace",
      summary = "Delete namespace metadata and its properties. Table rows remain unchanged.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "204",
        description = "Namespace metadata deleted.",
        content = @Content),
    @ApiResponse(
        responseCode = "404",
        description = "Namespace metadata was not found.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
  })
  @DeleteMapping("/{namespaceName}")
  public ResponseEntity<Void> deleteNamespace(@PathVariable("namespaceName") String namespaceName)
      throws NamespaceException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> response =
        namespacesApiHandler.deleteNamespace(namespaceName);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(
      operationId = "updateNamespaceProperties",
      tags = "Namespace",
      summary = "Atomically apply sparse property updates and removals.")
  @ApiResponses({
    @ApiResponse(responseCode = "200", description = "Namespace properties updated."),
    @ApiResponse(
        responseCode = "404",
        description = "Namespace metadata was not found.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class))),
    @ApiResponse(
        responseCode = "422",
        description = "Property updates and removals overlap.",
        content = @Content(schema = @Schema(implementation = NamespaceErrorResponse.class)))
  })
  @PostMapping(value = "/{namespaceName}/properties", consumes = "application/json")
  public ResponseEntity<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      @PathVariable("namespaceName") String namespaceName,
      @RequestBody UpdateNamespacePropertiesRequestBody request)
      throws NamespaceException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<UpdateNamespacePropertiesResponseBody>
        response = namespacesApiHandler.updateNamespaceProperties(namespaceName, request);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Hidden
  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceNotFound(
      NotFoundException exception) {
    return renderFailure(
        HttpStatus.NOT_FOUND, "NotFoundException", "Namespace metadata does not exist.");
  }

  @Hidden
  @ExceptionHandler(AlreadyExistsException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceDuplicate() {
    return renderFailure(
        HttpStatus.CONFLICT, "AlreadyExistsException", "Namespace name already exists.");
  }

  @Hidden
  @ExceptionHandler(VersionConflictException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceConflict() {
    return renderFailure(
        HttpStatus.CONFLICT,
        "VersionConflictException",
        "Namespace metadata changed concurrently.");
  }

  @Hidden
  @ExceptionHandler(InvalidRequestException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidNamespaceRequest(
      InvalidRequestException exception) {
    return renderFailure(HttpStatus.BAD_REQUEST, "InvalidRequestException", exception.getMessage());
  }

  @Hidden
  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidRequestBody() {
    return renderFailure(
        HttpStatus.BAD_REQUEST, "InvalidRequestException", "Invalid namespace request.");
  }

  @Hidden
  @ExceptionHandler(InvalidPropertyUpdateException.class)
  public ResponseEntity<NamespaceErrorResponse> handleInvalidNamespacePropertyUpdate(
      InvalidPropertyUpdateException exception) {
    return renderFailure(
        HttpStatus.UNPROCESSABLE_ENTITY,
        "InvalidPropertyUpdateException",
        "Property updates and removals must have disjoint keys.");
  }

  @Hidden
  @ExceptionHandler(UnavailableException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceMetadataUnavailable(
      UnavailableException exception) {
    return renderFailure(
        HttpStatus.SERVICE_UNAVAILABLE,
        "UnavailableException",
        "Namespace metadata service unavailable.");
  }

  @Hidden
  @ExceptionHandler(ServiceFailureException.class)
  public ResponseEntity<NamespaceErrorResponse> handleNamespaceMetadataStorage(
      ServiceFailureException exception) {
    return renderFailure(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "ServiceFailureException",
        "Namespace metadata operation failed.");
  }

  @Hidden
  @ExceptionHandler(MutationUncertainException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUncertainMutation() {
    return renderFailure(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "MutationUncertainException",
        "Namespace mutation outcome is unknown. Reload metadata and reconcile before retrying.");
  }

  @Hidden
  @ExceptionHandler({AuthenticationRequiredException.class, AuthenticationException.class})
  public ResponseEntity<NamespaceErrorResponse> handleAuthenticationRequired() {
    return renderFailure(
        HttpStatus.UNAUTHORIZED, "AuthenticationRequiredException", "Authentication required.");
  }

  @Hidden
  @ExceptionHandler({PermissionDeniedException.class, AccessDeniedException.class})
  public ResponseEntity<NamespaceErrorResponse> handlePermissionDenied() {
    return renderFailure(HttpStatus.FORBIDDEN, "PermissionDeniedException", "Access denied.");
  }

  @Hidden
  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<NamespaceErrorResponse> handleUnexpectedFailure(
      HttpServletRequest request) {
    if (!"GET".equals(request.getMethod())) {
      return handleUncertainMutation();
    }
    return renderFailure(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "ServiceFailureException",
        "Namespace metadata operation failed.");
  }

  private ResponseEntity<NamespaceErrorResponse> renderFailure(
      HttpStatus status, String type, String message) {
    return ResponseEntity.status(status)
        .body(new NamespaceErrorResponse(status, status.getReasonPhrase(), message, type));
  }
}
