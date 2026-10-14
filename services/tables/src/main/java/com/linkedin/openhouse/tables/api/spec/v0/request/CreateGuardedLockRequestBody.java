package com.linkedin.openhouse.tables.api.spec.v0.request;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CreateGuardedLockRequestBody {
  @Schema(description = "Expected table UUID from GET table.", required = true)
  private String expectedTableUUID;

  @Schema(
      description =
          "Caller-selected lock identity, stable across retries and distinct from other workflows.",
      required = true)
  private String lockId;

  @Schema(description = "Human-readable lock description.")
  private String message;
}
