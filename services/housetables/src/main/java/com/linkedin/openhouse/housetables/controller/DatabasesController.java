package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.housetables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.api.spec.request.CreateDatabaseRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateDatabasePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;
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
@RequestMapping(value = "/hts/databases", produces = "application/json")
@ApiResponses({
  @ApiResponse(
      responseCode = "400",
      description = "Invalid database request.",
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
  @ApiResponse(
      responseCode = "503",
      description = "Database metadata is unavailable for this backend.",
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
  @ApiResponse(
      responseCode = "500",
      description = "Database metadata storage failed.",
      content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
})
public class DatabasesController {
  private final DatabasesApiHandler databasesApiHandler;

  public DatabasesController(DatabasesApiHandler databasesApiHandler) {
    this.databasesApiHandler = databasesApiHandler;
  }

  @Operation(
      operationId = "createDatabase",
      tags = "Database",
      summary = "Create database metadata.")
  @ApiResponses({
    @ApiResponse(responseCode = "201", description = "Database metadata created."),
    @ApiResponse(
        responseCode = "409",
        description = "Database name already exists.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @PostMapping(consumes = "application/json")
  public ResponseEntity<DatabaseMetadata> createDatabase(
      @RequestBody CreateDatabaseRequestBody request) throws DatabaseMetadataException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<DatabaseMetadata> response =
        databasesApiHandler.createDatabase(request);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(operationId = "getDatabase", tags = "Database", summary = "Get database metadata.")
  @ApiResponses({
    @ApiResponse(responseCode = "200", description = "Database metadata loaded."),
    @ApiResponse(
        responseCode = "404",
        description = "Database metadata was not found.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @GetMapping("/{databaseName}")
  public ResponseEntity<DatabaseMetadata> getDatabase(
      @PathVariable("databaseName") String databaseName) throws DatabaseMetadataException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<DatabaseMetadata> response =
        databasesApiHandler.getDatabase(databaseName);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(
      operationId = "deleteDatabase",
      tags = "Database",
      summary = "Delete database metadata and its properties. Table rows remain unchanged.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "204",
        description = "Database metadata deleted.",
        content = @Content),
    @ApiResponse(
        responseCode = "404",
        description = "Database metadata was not found.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Database metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @DeleteMapping("/{databaseName}")
  public ResponseEntity<Void> deleteDatabase(@PathVariable("databaseName") String databaseName)
      throws DatabaseMetadataException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> response =
        databasesApiHandler.deleteDatabase(databaseName);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Operation(
      operationId = "updateDatabaseProperties",
      tags = "Database",
      summary = "Atomically apply sparse property updates and removals.")
  @ApiResponses({
    @ApiResponse(responseCode = "200", description = "Database properties updated."),
    @ApiResponse(
        responseCode = "404",
        description = "Database metadata was not found.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "409",
        description = "Database metadata changed concurrently.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class))),
    @ApiResponse(
        responseCode = "422",
        description = "Property updates and removals overlap.",
        content = @Content(schema = @Schema(implementation = ErrorResponseBody.class)))
  })
  @PostMapping(value = "/{databaseName}/properties", consumes = "application/json")
  public ResponseEntity<UpdateDatabasePropertiesResponseBody> updateDatabaseProperties(
      @PathVariable("databaseName") String databaseName,
      @RequestBody UpdateDatabasePropertiesRequestBody request)
      throws DatabaseMetadataException {
    com.linkedin.openhouse.common.api.spec.ApiResponse<UpdateDatabasePropertiesResponseBody>
        response = databasesApiHandler.updateDatabaseProperties(databaseName, request);
    return new ResponseEntity<>(
        response.getResponseBody(), response.getHttpHeaders(), response.getHttpStatus());
  }

  @Hidden
  @ExceptionHandler(DatabaseMetadataException.class)
  public ResponseEntity<ErrorResponseBody> handleDatabaseMetadataException(
      DatabaseMetadataException exception) {
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
