package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.housetables.api.handler.NamespacesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.services.InvalidNamespacePropertyUpdateException;
import com.linkedin.openhouse.housetables.services.InvalidNamespaceRequestException;
import com.linkedin.openhouse.housetables.services.NamespaceAlreadyExistsException;
import com.linkedin.openhouse.housetables.services.NamespaceConcurrentModificationException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataStorageException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataUnavailableException;
import com.linkedin.openhouse.housetables.services.NamespaceNotFoundException;
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
    return namespacesApiHandler.createNamespace(request).toResponseEntity();
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
    return namespacesApiHandler.getNamespace(namespaceName).toResponseEntity();
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
    return namespacesApiHandler.deleteNamespace(namespaceName).toResponseEntity();
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
    return namespacesApiHandler
        .updateNamespaceProperties(namespaceName, request)
        .toResponseEntity();
  }

  @Hidden
  @ExceptionHandler(NamespaceNotFoundException.class)
  public ResponseEntity<ErrorResponseBody> handleNamespaceNotFound(
      NamespaceNotFoundException exception) {
    return renderFailure(exception, HttpStatus.NOT_FOUND);
  }

  @Hidden
  @ExceptionHandler({
    NamespaceAlreadyExistsException.class,
    NamespaceConcurrentModificationException.class
  })
  public ResponseEntity<ErrorResponseBody> handleNamespaceConflict(
      NamespaceMetadataException exception) {
    return renderFailure(exception, HttpStatus.CONFLICT);
  }

  @Hidden
  @ExceptionHandler(InvalidNamespaceRequestException.class)
  public ResponseEntity<ErrorResponseBody> handleInvalidNamespaceRequest(
      InvalidNamespaceRequestException exception) {
    return renderFailure(exception, HttpStatus.BAD_REQUEST);
  }

  @Hidden
  @ExceptionHandler(InvalidNamespacePropertyUpdateException.class)
  public ResponseEntity<ErrorResponseBody> handleInvalidNamespacePropertyUpdate(
      InvalidNamespacePropertyUpdateException exception) {
    return renderFailure(exception, HttpStatus.UNPROCESSABLE_ENTITY);
  }

  @Hidden
  @ExceptionHandler(NamespaceMetadataUnavailableException.class)
  public ResponseEntity<ErrorResponseBody> handleNamespaceMetadataUnavailable(
      NamespaceMetadataUnavailableException exception) {
    return renderFailure(exception, HttpStatus.SERVICE_UNAVAILABLE);
  }

  @Hidden
  @ExceptionHandler(NamespaceMetadataStorageException.class)
  public ResponseEntity<ErrorResponseBody> handleNamespaceMetadataStorage(
      NamespaceMetadataStorageException exception) {
    return renderFailure(exception, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  private ResponseEntity<ErrorResponseBody> renderFailure(
      NamespaceMetadataException exception, HttpStatus status) {
    return ResponseEntity.status(status)
        .body(
            ErrorResponseBody.builder()
                .status(status)
                .error(status.getReasonPhrase())
                .message(exception.getMessage())
                .build());
  }
}
