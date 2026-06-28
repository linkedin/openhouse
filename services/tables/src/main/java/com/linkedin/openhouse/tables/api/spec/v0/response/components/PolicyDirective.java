package com.linkedin.openhouse.tables.api.spec.v0.response.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * A single, feature-namespaced directive carried by {@link RuntimePolicy}.
 *
 * <p>The fields here are a small, typed, feature-agnostic envelope shared across all features; the
 * per-feature semantics live entirely inside the opaque {@link #payload} JSON. Keeping the payload
 * opaque is what lets a new feature ride this channel as a new {@code directives} map entry rather
 * than an API/schema change.
 *
 * <p>READ_ONLY and advisory by default. A client that does not recognize a directive MUST ignore it
 * — it cannot honor what it does not understand, so {@code REQUIRED} only binds clients new enough
 * to know the directive. Directives whose correctness must be guaranteed are additionally enforced
 * server-side; the client-visible copy is then a hint, not the authority.
 */
@Builder
@Value
public class PolicyDirective {
  @Schema(
      description =
          "Whether the client must honor this directive when it understands it. "
              + "ADVISORY: client MAY honor; ignoring it is safe. "
              + "REQUIRED: client SHOULD honor; correctness is additionally enforced server-side. "
              + "Unknown values MUST be treated as ADVISORY.",
      example = "ADVISORY")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private String enforcement;

  @Schema(
      description =
          "Minimum client version that should act on this directive; the reader gates it out for "
              + "older clients. Absent means no lower bound.",
      example = "1.2.1")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private String minClientVersion;

  @Schema(
      description = "Human-readable reason for this directive; for observability only.",
      example = "canary")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private String reason;

  @Schema(
      description =
          "Opaque, feature-specific JSON payload. The envelope never interprets it; the consuming "
              + "client feature parses it leniently. Keeping it opaque is what lets new features ride "
              + "this channel with no API/schema change.",
      example = "{\"read\":\"ON\",\"write\":\"AUDIT\"}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private String payload;
}
