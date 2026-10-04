package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;

/** Request to compare-and-set the per-edge replication checkpoint after a destination commit. */
@Builder(toBuilder = true)
@Value
public class ReplicationCheckpointUpdate {
  @JsonProperty("sourceClusterId")
  @NotBlank
  String sourceClusterId;

  @JsonProperty("sourceTableUUID")
  @NotBlank
  String sourceTableUUID;

  @JsonProperty("sourceCreationTime")
  @NotNull
  Long sourceCreationTime;

  @JsonProperty("destinationClusterId")
  @NotBlank
  String destinationClusterId;

  @JsonProperty("destinationTableUUID")
  @NotBlank
  String destinationTableUUID;

  @JsonProperty("destinationCreationTime")
  @NotNull
  Long destinationCreationTime;

  @JsonProperty("sourceDatabaseId")
  @NotBlank
  String sourceDatabaseId;

  @JsonProperty("sourceTableId")
  @NotBlank
  String sourceTableId;

  @JsonProperty("destinationDatabaseId")
  @NotBlank
  String destinationDatabaseId;

  @JsonProperty("destinationTableId")
  @NotBlank
  String destinationTableId;

  @JsonProperty("expectedRevision")
  @NotNull
  Long expectedRevision;

  @JsonProperty("sourceTableVersion")
  @NotBlank
  String sourceTableVersion;

  @JsonProperty("sourceSnapshotId")
  @NotNull
  Long sourceSnapshotId;

  @JsonProperty("destinationSnapshotId")
  @NotNull
  Long destinationSnapshotId;

  @JsonProperty("destinationTableVersion")
  @NotBlank
  String destinationTableVersion;
}
