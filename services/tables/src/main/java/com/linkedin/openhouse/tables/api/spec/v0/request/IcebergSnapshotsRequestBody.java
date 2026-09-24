package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.ArraySchema;
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

  @Schema(description = "Expected base metadata version for the entire atomic transaction")
  @NotEmpty(message = "baseTableVersion cannot be empty")
  private String baseTableVersion;

  @Schema(description = "Legacy full snapshot state; ignored when updates is present")
  private List<String> jsonSnapshots;

  @Schema(
      description =
          "Legacy full snapshot-ref state; ignored when updates is present. "
              + "Key is the ref name, and value is the JSON-serialized SnapshotRef.")
  private Map<String, String> snapshotRefs;

  /**
   * One atomic transaction of ordered Iceberg REST {@code TableUpdate} objects.
   *
   * <p>When present, every action is validated and applied in order against {@link
   * #baseTableVersion}, and the resulting metadata is published once. Multiple refs and repeated
   * changes to the same ref are preserved. Unknown or invalid actions reject the whole request. An
   * empty list is an explicit transaction with no Iceberg mutations, not a legacy request.
   *
   * <p>Absent/null retains the legacy full-state protocol. The OpenHouse envelope still supplies
   * governance policies and table identity; its schema/properties and the full-state snapshot
   * fields do not replace mutations supplied here.
   */
  @ArraySchema(
      arraySchema =
          @Schema(
              description =
                  "Optional ordered Iceberg REST TableUpdate objects. When present, all actions "
                      + "are authoritative and commit atomically against baseTableVersion. "
                      + "Invalid or unsupported actions reject the whole transaction. "
                      + "Absent/null selects legacy full-state handling; an empty array does not."),
      schema =
          @Schema(
              type = "object",
              description =
                  "One Iceberg REST TableUpdate, discriminated by action (e.g. add-snapshot, "
                      + "set-snapshot-ref)."))
  private List<Map<String, Object>> updates;

  @Schema(description = "The request body that contains complete metadata")
  private CreateUpdateTableRequestBody createUpdateTableRequestBody;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
