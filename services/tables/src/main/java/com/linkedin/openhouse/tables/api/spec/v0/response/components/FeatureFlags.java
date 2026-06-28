package com.linkedin.openhouse.tables.api.spec.v0.response.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Generic, server-stamped feature flags delivered to clients on table responses.
 *
 * <p>This is the delivery channel for per-table, runtime client behavior the server controls
 * <em>without a client re-roll</em>: the server stamps it onto every table response, and a client
 * reads it live on each table load (and on commit responses). It is intentionally a small TYPED
 * envelope wrapping OPAQUE, feature-namespaced payloads ({@link FeatureFlag#getPayload()}), so
 * adding a new feature is a new {@link #flags} entry rather than an API/schema change.
 *
 * <p>Forward-compatibility contract — clients rely on these, so they must never be weakened:
 *
 * <ul>
 *   <li>Absence — a null envelope, a missing flag key, or a {@code version} newer than the client
 *       understands — means the client's existing safe default, never a behavior change.
 *   <li>The envelope is additive-only; existing fields are never repurposed.
 *   <li>Unknown flag keys are ignored, even when marked {@code REQUIRED}.
 *   <li>READ_ONLY and side-channel: it is never accepted on requests, never echoed back on writes,
 *       and never persisted into table metadata.
 * </ul>
 *
 * <p>Not to be confused with the table-governance {@code Policies} object (retention, sharing,
 * replication, history); this carries client runtime behavior, not table governance.
 */
@Builder
@Value
public class FeatureFlags {
  @Schema(
      description =
          "Envelope format version. Additive-only; a client treats a newer version as a superset and "
              + "parses leniently.",
      example = "1")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private int version;

  @Schema(
      description =
          "Server epoch time in milliseconds at which these flags were stamped; for staleness "
              + "reasoning and metrics only.",
      example = "1718900000000")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long serverTimeMs;

  @Schema(
      description =
          "Feature flags keyed by feature namespace (e.g. \"read-bridge\"). A client looks up only "
              + "the namespaces it understands and ignores the rest.")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private Map<String, FeatureFlag> flags;
}
