package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.handler.ToggleStatusesApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.TableToggleStatusKey;
import com.linkedin.openhouse.housetables.api.spec.model.ToggleStatus;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Toggle Statuses are read-only for HTS, thus create/update paths are intentionally not
 * implemented.
 */
@RestController
public class ToggleStatusesController {

  private static final String TOGGLE_ENDPOINT = "/hts/togglestatuses";

  @Autowired private ToggleStatusesApiHandler toggleStatuesApiHandler;

  @Operation(
      summary = "Get a toggle status applied to a table.",
      description = "Returns a toggle status of databaseID and tableId on a featureId",
      tags = {"ToggleStatus"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Toggle status GET: OK"),
        @ApiResponse(responseCode = "404", description = "Toggle status GET: NOT FOUND")
      })
  @GetMapping(
      value = TOGGLE_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<EntityResponseBody<ToggleStatus>> getTableToggleStatus(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId,
      @RequestParam(value = "featureId") String featureId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<ToggleStatus>>
        apiResponse =
            toggleStatuesApiHandler.getEntity(
                TableToggleStatusKey.builder()
                    .databaseId(databaseId)
                    .tableId(tableId)
                    .featureId(featureId)
                    .build());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
