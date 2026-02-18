package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.IcebergMetadataApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetMetadataDiffResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableDataResponseBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal Tables Controller for rich metadata access. Provides detailed Iceberg metadata including
 * full metadata.json and history. This class delegates business logic to InternalTablesApiHandler.
 */
@RestController
public class IcebergMetadataController {

  @Autowired private IcebergMetadataApiHandler icebergMetadataApiHandler;

  @Operation(
      summary = "Get Table Metadata (Internal)",
      description =
          "Returns Iceberg metadata including full metadata.json content and history. "
              + "This is an internal endpoint for detailed table inspection.",
      tags = {"Internal"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Metadata GET: OK"),
        @ApiResponse(responseCode = "404", description = "Metadata GET: NOT_FOUND"),
        @ApiResponse(responseCode = "500", description = "Metadata GET: INTERNAL_ERROR")
      })
  @GetMapping(
      value = {"/internal/tables/{databaseId}/{tableId}/metadata"},
      produces = {"application/json"})
  public ResponseEntity<GetIcebergMetadataResponseBody> getIcebergMetadata(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetIcebergMetadataResponseBody> apiResponse =
        icebergMetadataApiHandler.getIcebergMetadata(
            databaseId, tableId, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get Metadata Diff for Metadata File (Internal)",
      description =
          "Returns the metadata diff between a specific metadata file and its immediate predecessor. "
              + "Includes both current and previous metadata.json content for client-side diffing. "
              + "This is an internal endpoint for detailed metadata inspection.",
      tags = {"Internal"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Metadata Diff GET: OK"),
        @ApiResponse(responseCode = "404", description = "Metadata Diff GET: NOT_FOUND"),
        @ApiResponse(responseCode = "400", description = "Metadata Diff GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "500", description = "Metadata Diff GET: INTERNAL_ERROR")
      })
  @GetMapping(
      value = {"/internal/tables/{databaseId}/{tableId}/metadata/diff"},
      produces = {"application/json"})
  public ResponseEntity<GetMetadataDiffResponseBody> getMetadataDiff(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(description = "Metadata file location to compare", required = true) @RequestParam
          String metadataFile) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetMetadataDiffResponseBody> apiResponse =
        icebergMetadataApiHandler.getMetadataDiff(
            databaseId, tableId, metadataFile, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get Table Data (Internal)",
      description =
          "Returns the first N rows of data from an Iceberg table. "
              + "This is an internal endpoint for table data preview.",
      tags = {"Internal"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Table Data GET: OK"),
        @ApiResponse(responseCode = "404", description = "Table Data GET: NOT_FOUND"),
        @ApiResponse(responseCode = "400", description = "Table Data GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "500", description = "Table Data GET: INTERNAL_ERROR")
      })
  @GetMapping(
      value = {"/internal/tables/{databaseId}/{tableId}/data"},
      produces = {"application/json"})
  public ResponseEntity<GetTableDataResponseBody> getTableData(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(description = "Maximum number of rows to return (default: 100, max: 1000)")
          @RequestParam(defaultValue = "100")
          int limit) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableDataResponseBody> apiResponse =
        icebergMetadataApiHandler.getTableData(
            databaseId, tableId, limit, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
