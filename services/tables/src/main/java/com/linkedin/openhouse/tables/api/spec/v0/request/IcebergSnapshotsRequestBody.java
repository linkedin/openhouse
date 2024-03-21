package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class IcebergSnapshotsRequestBody {

  @Schema(description = "Base Table Version", example = "Base table version to apply the change to")
  @NotEmpty(message = "baseTableVersion cannot be empty")
  private String baseTableVersion;

  @Schema(description = "List of json serialized snapshots to put")
  private List<String> jsonSnapshots;

  @Schema(
      description =
          "Map of branch name to json serialized SnapshotRef. "
              + "Key is the branch name, and value is the SnapshotRef.")
  private Map<String, String> snapshotRefs;

  @Schema(description = "The request body that contains complete metadata")
  private CreateUpdateTableRequestBody createUpdateTableRequestBody;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
