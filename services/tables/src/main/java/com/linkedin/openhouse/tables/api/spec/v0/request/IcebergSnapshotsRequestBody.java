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

  /**
   * The deltas this commit applies, as Iceberg REST {@code CommitTableRequest.updates[]}.
   *
   * <p>Each element is one {@code TableUpdate} object (discriminated by {@code action}: {@code
   * add-snapshot}, {@code set-snapshot-ref}, {@code remove-snapshot-ref}, …). The wire type is an
   * array of objects, not an array of JSON strings, so this field <em>is</em> {@code updates[]} —
   * same name, same item shape. Convergence is then dropping {@link #jsonSnapshots}/{@link
   * #snapshotRefs}/{@link #baseTableVersion} and adding {@code requirements[]}, not a rename or a
   * type change.
   *
   * <p>Unlike the full-state fields — which force the server to rediscover what changed by diffing
   * — this field states the change. A {@code CREATE BRANCH b} that commits no snapshot appears as a
   * single {@code set-snapshot-ref} naming {@code b}.
   *
   * <p>Optional and advisory in this release: table metadata is still built from {@code
   * jsonSnapshots}/{@code snapshotRefs}, and clients predating this field omit it. Consumers must
   * tolerate null/empty and must not fail the commit on an unknown or unparseable action. When this
   * field becomes authoritative, the REST rule applies: unknown updates MUST 400.
   */
  @ArraySchema(
      arraySchema =
          @Schema(
              description =
                  "Optional. Iceberg REST CommitTableRequest.updates[]: TableUpdate objects "
                      + "for the deltas this commit applies. Advisory only: table metadata is "
                      + "still built from jsonSnapshots/snapshotRefs. Older clients omit this "
                      + "field. When this field becomes authoritative, unknown updates MUST 400."),
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
