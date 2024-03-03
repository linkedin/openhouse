package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.spec.v1.request.DeltaActionsRequestBody;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.DeltaActionsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeltaActionsController {

  @Autowired private DeltaActionsService deltaActionsService;

  @Autowired private TablesMapper tablesMapper;

  @Operation(
      summary = "Puts Delta snapshots to Table",
      description = "Puts Delta snapshots to Table",
      tags = {"DeltaSnapshot"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "Delta snapshot PATCH: CREATED"),
        @ApiResponse(responseCode = "200", description = "Delta snapshot PATCH: UPDATED"),
        @ApiResponse(responseCode = "400", description = "Delta snapshot PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "409", description = "Delta snapshot PATCH: CONFLICT")
      })
  @PatchMapping(
      value = {"/v1/databases/{databaseId}/tables/{tableId}/delta/v1/actions"},
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<GetTableResponseBody> patchDeltaActions(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(
              description =
                  "Request containing a list of JSON serialized Iceberg Snapshots to be put",
              required = true,
              schema = @Schema(implementation = DeltaActionsRequestBody.class))
          @RequestBody
          DeltaActionsRequestBody deltaActionsRequestBody) {
    TableDto tableDto =
        deltaActionsService.appendDeltaActions(
            databaseId, tableId, deltaActionsRequestBody, extractAuthenticatedUserPrincipal());

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableResponseBody> apiResponse =
        com.linkedin.openhouse.common.api.spec.ApiResponse.<GetTableResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(tablesMapper.toGetTableResponseBody(tableDto))
            .build();

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
