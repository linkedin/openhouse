package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.client.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.client.model.ReplicationEdgeState;
import com.linkedin.openhouse.tables.services.ReplicationStateCatalogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Authorized Tables API for replication destination associations and checkpoints. */
@RestController
public class ReplicationStateController {
  private static final String DESTINATION_ENDPOINT = "/v1/replication/destinations";
  private static final String CHECKPOINT_ENDPOINT = "/v1/replication/checkpoints";

  @Autowired private ReplicationStateCatalogService replicationStateCatalogService;

  @Operation(
      summary = "Get destinations and checkpoint state for a source table generation",
      description =
          "Looks up replication edges by the immutable source cluster, UUID, and creation time.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Replication state found"),
        @ApiResponse(responseCode = "403", description = "System administrator privilege required")
      })
  @GetMapping(value = DESTINATION_ENDPOINT, produces = "application/json")
  public ResponseEntity<List<ReplicationEdgeState>> getDestinations(
      @RequestParam("sourceClusterId") String sourceClusterId,
      @RequestParam("sourceTableUUID") String sourceTableUUID,
      @RequestParam("sourceCreationTime") long sourceCreationTime) {
    return ResponseEntity.ok(
        replicationStateCatalogService.getDestinations(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            extractAuthenticatedUserPrincipal()));
  }

  @Operation(
      summary = "Register a destination or update catalog locators",
      description =
          "Requires SYSTEM_ADMIN on the destination table. Locator changes do not modify its checkpoint.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Destination association stored"),
        @ApiResponse(responseCode = "403", description = "System administrator privilege required"),
        @ApiResponse(
            responseCode = "409",
            description = "Stale locator revision or table generation")
      })
  @PutMapping(
      value = DESTINATION_ENDPOINT,
      consumes = "application/json",
      produces = "application/json")
  public ResponseEntity<ReplicationDestination> putDestination(
      @Valid @RequestBody ReplicationDestination destination) {
    return ResponseEntity.ok(
        replicationStateCatalogService.putDestination(
            destination, extractAuthenticatedUserPrincipal()));
  }

  @Operation(
      summary = "Get checkpoint state for a source-to-destination edge",
      description = "Requires SYSTEM_ADMIN on the destination table.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Checkpoint found"),
        @ApiResponse(responseCode = "403", description = "System administrator privilege required"),
        @ApiResponse(responseCode = "404", description = "Checkpoint or destination not found")
      })
  @GetMapping(value = CHECKPOINT_ENDPOINT, produces = "application/json")
  public ResponseEntity<ReplicationCheckpoint> getCheckpoint(
      @RequestParam("sourceClusterId") String sourceClusterId,
      @RequestParam("sourceTableUUID") String sourceTableUUID,
      @RequestParam("sourceCreationTime") long sourceCreationTime,
      @RequestParam("destinationClusterId") String destinationClusterId,
      @RequestParam("destinationTableUUID") String destinationTableUUID,
      @RequestParam("destinationCreationTime") long destinationCreationTime) {
    return ResponseEntity.ok(
        replicationStateCatalogService.getCheckpoint(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            destinationClusterId,
            destinationTableUUID,
            destinationCreationTime,
            extractAuthenticatedUserPrincipal()));
  }

  @Operation(
      summary = "Compare-and-set checkpoint after a successful snapshot commit",
      description =
          "Requires SYSTEM_ADMIN on the destination table and an exact match with its current table version. Unknown commit outcomes must be reconciled before this call.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Checkpoint advanced"),
        @ApiResponse(responseCode = "403", description = "System administrator privilege required"),
        @ApiResponse(responseCode = "409", description = "Stale checkpoint or destination version"),
        @ApiResponse(responseCode = "404", description = "Destination association not found")
      })
  @PutMapping(
      value = CHECKPOINT_ENDPOINT,
      consumes = "application/json",
      produces = "application/json")
  public ResponseEntity<ReplicationCheckpoint> advanceCheckpoint(
      @Valid @RequestBody ReplicationCheckpointUpdate update) {
    return ResponseEntity.ok(
        replicationStateCatalogService.advanceCheckpoint(
            update, extractAuthenticatedUserPrincipal()));
  }
}
