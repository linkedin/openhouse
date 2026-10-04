package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.housetables.repository.ReplicationConfigurationStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import javax.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReplicationConfigurationController {
  private static final String REPLICATION_CONFIGURATION_ENDPOINT =
      "/hts/replication-configurations";

  private final ReplicationConfigurationStore replicationConfigurationStore;

  public ReplicationConfigurationController(
      ReplicationConfigurationStore replicationConfigurationStore) {
    this.replicationConfigurationStore = replicationConfigurationStore;
  }

  @Operation(
      summary = "Get the catalog replication configuration for a source table",
      tags = {"Replication Configuration"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Replication configuration found"),
        @ApiResponse(responseCode = "404", description = "No catalog state for this source table")
      })
  @GetMapping(value = REPLICATION_CONFIGURATION_ENDPOINT, produces = "application/json")
  public ResponseEntity<ReplicationConfigurationSet> getReplicationConfiguration(
      @RequestParam("sourceDatabaseId") String sourceDatabaseId,
      @RequestParam("sourceTableId") String sourceTableId) {
    return replicationConfigurationStore
        .findBySource(sourceDatabaseId, sourceTableId)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @Operation(
      summary = "Replace the catalog replication configuration for a source table",
      tags = {"Replication Configuration"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "Replication configuration replaced"),
        @ApiResponse(responseCode = "400", description = "Invalid replication configuration")
      })
  @PutMapping(value = REPLICATION_CONFIGURATION_ENDPOINT, consumes = "application/json")
  public ResponseEntity<Void> replaceReplicationConfiguration(
      @Valid @RequestBody ReplicationConfigurationSet replicationConfigurationSet) {
    replicationConfigurationStore.replace(replicationConfigurationSet);
    return ResponseEntity.noContent().build();
  }
}
