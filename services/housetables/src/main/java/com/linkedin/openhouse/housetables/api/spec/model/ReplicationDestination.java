package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;

/** Stable source-to-destination association with mutable catalog locators. */
@Builder(toBuilder = true)
@Value
public class ReplicationDestination {
  @Schema(example = "source-cluster")
  @JsonProperty("sourceClusterId")
  @NotBlank
  String sourceClusterId;

  @Schema(example = "73ea0d21-3c89-4987-a6cf-26e4f86bdcee")
  @JsonProperty("sourceTableUUID")
  @NotBlank
  String sourceTableUUID;

  @Schema(example = "1651002318265")
  @JsonProperty("sourceCreationTime")
  @NotNull
  Long sourceCreationTime;

  @Schema(example = "source_db")
  @JsonProperty("sourceDatabaseId")
  @NotBlank
  String sourceDatabaseId;

  @Schema(example = "source_table")
  @JsonProperty("sourceTableId")
  @NotBlank
  String sourceTableId;

  @Schema(example = "destination-cluster")
  @JsonProperty("destinationClusterId")
  @NotBlank
  String destinationClusterId;

  @Schema(example = "2d8a7fb9-0b23-4419-a432-6f3b9b3c9226")
  @JsonProperty("destinationTableUUID")
  @NotBlank
  String destinationTableUUID;

  @Schema(example = "1651002318265")
  @JsonProperty("destinationCreationTime")
  @NotNull
  Long destinationCreationTime;

  @Schema(example = "destination_db")
  @JsonProperty("destinationDatabaseId")
  @NotBlank
  String destinationDatabaseId;

  @Schema(example = "destination_table")
  @JsonProperty("destinationTableId")
  @NotBlank
  String destinationTableId;

  @Schema(description = "Current locator revision; use 0 when creating the association.")
  @JsonProperty("expectedVersion")
  @NotNull
  Long expectedVersion;

  @Schema(accessMode = Schema.AccessMode.READ_ONLY)
  @JsonProperty(value = "version", access = JsonProperty.Access.READ_ONLY)
  Long version;
}
