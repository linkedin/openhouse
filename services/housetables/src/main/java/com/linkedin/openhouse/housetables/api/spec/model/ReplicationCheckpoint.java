package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

/** Per-edge progress; this resource is stored separately from the destination association. */
@Builder(toBuilder = true)
@Value
public class ReplicationCheckpoint {
  @JsonProperty("sourceClusterId")
  String sourceClusterId;

  @JsonProperty("sourceTableUUID")
  String sourceTableUUID;

  @JsonProperty("sourceCreationTime")
  Long sourceCreationTime;

  @JsonProperty("destinationClusterId")
  String destinationClusterId;

  @JsonProperty("destinationTableUUID")
  String destinationTableUUID;

  @JsonProperty("destinationCreationTime")
  Long destinationCreationTime;

  @Schema(
      description =
          "Source metadata version observed by the worker. Unlike a snapshot ID, this changes for schema-only commits.")
  @JsonProperty("sourceTableVersion")
  String sourceTableVersion;

  @JsonProperty("sourceSnapshotId")
  Long sourceSnapshotId;

  @JsonProperty("destinationSnapshotId")
  Long destinationSnapshotId;

  @JsonProperty("destinationTableVersion")
  String destinationTableVersion;

  @Schema(description = "Monotonically increasing per-edge compare-and-set revision.")
  @JsonProperty(value = "revision", access = JsonProperty.Access.READ_ONLY)
  Long revision;
}
