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

  /**
   * The deltas this commit applies, in Iceberg REST spec form.
   *
   * <p>Each element is one {@code TableUpdate} from the Iceberg REST catalog spec (an object with
   * an {@code action} discriminator, e.g. {@code add-snapshot}, {@code set-snapshot-ref}, {@code
   * remove-snapshot-ref}), serialized by {@code MetadataUpdateParser} so the wire format is
   * byte-identical to {@code CommitTableRequest.updates[]}.
   *
   * <p>Unlike {@link #jsonSnapshots} and {@link #snapshotRefs} — which carry complete replacement
   * state and therefore force the server to rediscover what changed by diffing — this field states
   * the change directly. A {@code CREATE BRANCH b} that commits no new snapshot appears here as a
   * single {@code set-snapshot-ref} naming {@code b}, which is otherwise unknowable server-side.
   *
   * <p>Optional and advisory in this release: the server still builds table metadata from {@code
   * jsonSnapshots}/{@code snapshotRefs}, and clients predating this field simply omit it. Consumers
   * must tolerate null/empty. This is the forward-compatible shape — when OpenHouse adopts the REST
   * {@code CommitTableRequest} endpoint, this field is promoted to {@code updates} and the
   * full-state fields retire.
   */
  @Schema(
      description =
          "Optional. Iceberg REST spec TableUpdate actions describing the deltas this commit "
              + "applies, each serialized by MetadataUpdateParser and wire-compatible with "
              + "CommitTableRequest.updates[]. Advisory only: table metadata is still built from "
              + "jsonSnapshots/snapshotRefs. Older clients omit this field.")
  private List<String> jsonMetadataUpdates;

  @Schema(description = "The request body that contains complete metadata")
  private CreateUpdateTableRequestBody createUpdateTableRequestBody;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
