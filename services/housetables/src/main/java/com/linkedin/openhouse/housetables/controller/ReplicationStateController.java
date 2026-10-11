package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationEdgeState;
import com.linkedin.openhouse.housetables.repository.ReplicationStateStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.List;
import javax.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Internal House Tables API for stable replication associations and per-edge checkpoints. */
@RestController
public class ReplicationStateController {
  private static final String DESTINATION_ENDPOINT = "/hts/replication-destinations";
  private static final String CHECKPOINT_ENDPOINT = "/hts/replication-checkpoints";

  private final ReplicationStateStore replicationStateStore;

  public ReplicationStateController(ReplicationStateStore replicationStateStore) {
    this.replicationStateStore = replicationStateStore;
  }

  @Operation(
      summary = "Get destinations and checkpoint state for a source table generation",
      description =
          "Returns destination locators and optional checkpoint state for the immutable source identity.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Replication state found"),
        @ApiResponse(responseCode = "400", description = "Invalid source identity")
      })
  @GetMapping(value = DESTINATION_ENDPOINT, produces = "application/json")
  public ResponseEntity<List<ReplicationEdgeState>> getDestinations(
      @RequestParam("sourceClusterId") String sourceClusterId,
      @RequestParam("sourceTableUUID") String sourceTableUUID,
      @RequestParam("sourceCreationTime") long sourceCreationTime) {
    return ResponseEntity.ok(
        replicationStateStore.findDestinations(
            sourceClusterId, sourceTableUUID, sourceCreationTime));
  }

  @Operation(
      summary = "Register a destination or update current catalog locators",
      description =
          "Upserts the destination association using stable table generations and a locator CAS revision. Does not modify checkpoint progress.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Destination association stored"),
        @ApiResponse(responseCode = "400", description = "Invalid destination association"),
        @ApiResponse(responseCode = "409", description = "Stale locator revision")
      })
  @PutMapping(
      value = DESTINATION_ENDPOINT,
      consumes = "application/json",
      produces = "application/json")
  public ResponseEntity<ReplicationDestination> putDestination(
      @Valid @RequestBody ReplicationDestination destination) {
    return ResponseEntity.ok(replicationStateStore.putDestination(destination));
  }

  @Operation(
      summary = "Get the checkpoint for one source-to-destination edge",
      description =
          "Checkpoint identity is immutable across catalog renames; use the locator values returned by the destination lookup.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Checkpoint found"),
        @ApiResponse(responseCode = "404", description = "Checkpoint not found")
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
        replicationStateStore.findCheckpoint(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            destinationClusterId,
            destinationTableUUID,
            destinationCreationTime));
  }

  @Operation(
      summary = "Compare-and-set per-edge checkpoint after destination commit",
      description =
          "Call only after the standard destination snapshot PUT has succeeded. The destination table version is supplied by that commit response; retries are idempotent and stale revisions return 409.")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Checkpoint advanced"),
        @ApiResponse(responseCode = "400", description = "Invalid checkpoint"),
        @ApiResponse(responseCode = "409", description = "Stale checkpoint revision"),
        @ApiResponse(responseCode = "404", description = "Destination association not found")
      })
  @PutMapping(
      value = CHECKPOINT_ENDPOINT,
      consumes = "application/json",
      produces = "application/json")
  public ResponseEntity<ReplicationCheckpoint> advanceCheckpoint(
      @Valid @RequestBody ReplicationCheckpointUpdate update) {
    return ResponseEntity.ok(replicationStateStore.advanceCheckpoint(update));
  }
}
