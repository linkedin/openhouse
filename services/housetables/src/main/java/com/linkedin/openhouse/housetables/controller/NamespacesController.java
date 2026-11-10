package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
  @ApiResponse(
      responseCode = "503",
      description = "Namespace metadata is unavailable for this backend.",
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
  @ApiResponse(
      responseCode = "500",
      description = "Namespace metadata storage failed.",
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
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
        description = "Namespace name already exists.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @PostMapping(consumes = "application/json")
  public ResponseEntity<NamespaceMetadataResponse> createNamespace(
      @RequestBody CreateNamespaceRequestBody request) throws NamespaceMetadataException {
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
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @GetMapping("/{namespaceName}")
  public ResponseEntity<NamespaceMetadataResponse> getNamespace(
      @PathVariable("namespaceName") String namespaceName) throws NamespaceMetadataException {
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
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @DeleteMapping("/{namespaceName}")
  public ResponseEntity<Void> deleteNamespace(@PathVariable("namespaceName") String namespaceName)
      throws NamespaceMetadataException {
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
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Namespace metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "422",
        description = "Property updates and removals overlap.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @PostMapping(value = "/{namespaceName}/properties", consumes = "application/json")
  public ResponseEntity<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      @PathVariable("namespaceName") String namespaceName,
      @RequestBody UpdateNamespacePropertiesRequestBody request)
      throws NamespaceMetadataException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<UpdateNamespacePropertiesResponseBody>
        response = namespacesApiHandler.updateNamespaceProperties(namespaceName, request);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Hidden
  @ExceptionHandler(NamespaceMetadataException.class)
  public ResponseEntity<ErrorResponseBody> handleNamespaceMetadataException(
      NamespaceMetadataException exception) {
    HttpStatus status;
    switch (exception.getReason()) {
      case NOT_FOUND:
        status = HttpStatus.NOT_FOUND;
        break;
      case ALREADY_EXISTS:
      case CONCURRENT_MODIFICATION:
        status = HttpStatus.CONFLICT;
        break;
      case INVALID_REQUEST:
        status = HttpStatus.BAD_REQUEST;
        break;
      case INVALID_UPDATE:
        status = HttpStatus.UNPROCESSABLE_ENTITY;
        break;
      case UNAVAILABLE:
        status = HttpStatus.SERVICE_UNAVAILABLE;
        break;
      default:
        status = HttpStatus.INTERNAL_SERVER_ERROR;
        break;
    }
    return ResponseEntity.status(status)
        .body(
            ErrorResponseBody.builder()
                .status(status)
                .error(status.getReasonPhrase())
                .message(exception.getMessage())
                .build());
  }
}
