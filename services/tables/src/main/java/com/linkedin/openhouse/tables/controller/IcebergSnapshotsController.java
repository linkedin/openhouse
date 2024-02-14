package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.IcebergSnapshotsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * IcebergSnapshotsController is the controller class for Iceberg Snapshots API. This class is
 * responsible for handling all the API requests that are specific to Iceberg table format. The
 * class uses IcebergSnapshotsApiHandler to delegate the request to the service layer.
 */
@RestController
public class IcebergSnapshotsController {

  @Autowired private IcebergSnapshotsApiHandler icebergSnapshotsApiHandler;

  @Operation(
      summary = "Puts Iceberg snapshots to Table",
      description = "Puts Iceberg snapshots to Table",
      tags = {"Snapshot"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "Iceberg snapshot PUT: CREATED"),
        @ApiResponse(responseCode = "200", description = "Iceberg snapshot PUT: UPDATED"),
        @ApiResponse(responseCode = "400", description = "Iceberg snapshot PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "409", description = "Iceberg snapshot PUT: CONFLICT")
      })
  @PutMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots",
        "/v1/databases/{databaseId}/tables/{tableId}/iceberg/v2/snapshots"
      },
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<GetTableResponseBody> putSnapshots(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(
              description =
                  "Request containing a list of JSON serialized Iceberg Snapshots to be put",
              required = true,
              schema = @Schema(implementation = IcebergSnapshotsRequestBody.class))
          @RequestBody
          IcebergSnapshotsRequestBody icebergsSnapshotRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableResponseBody> apiResponse =
        icebergSnapshotsApiHandler.putIcebergSnapshots(
            databaseId, tableId, icebergsSnapshotRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<GetTableResponseBody>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
