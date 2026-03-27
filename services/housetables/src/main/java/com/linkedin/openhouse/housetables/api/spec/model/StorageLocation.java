package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Builder;
import lombok.Value;

/** API model for a StorageLocation — a URI that an Iceberg table's data files reside at. */
@Builder(toBuilder = true)
@Value
public class StorageLocation {

  @Schema(description = "Unique identifier for this storage location.", example = "a1b2c3d4-...")
  @JsonProperty(value = "storageLocationId")
  private String storageLocationId;

  @Schema(
      description =
          "URI of the storage location. Opaque to OpenHouse; used as Iceberg metadata.location.",
      example = "/data/openhouse/mydb/mytable-a1b2c3d4")
  @JsonProperty(value = "uri")
  @NotEmpty(message = "uri cannot be empty")
  private String uri;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
