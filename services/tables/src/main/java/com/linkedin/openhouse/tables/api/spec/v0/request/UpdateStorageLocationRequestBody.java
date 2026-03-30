package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.GsonBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Request body for updating a table's active storage location. */
@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UpdateStorageLocationRequestBody {

  @Schema(
      description = "ID of the StorageLocation to set as the table's active write target.",
      example = "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
  @JsonProperty(value = "storageLocationId")
  @NotEmpty(message = "storageLocationId cannot be empty")
  private String storageLocationId;

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
