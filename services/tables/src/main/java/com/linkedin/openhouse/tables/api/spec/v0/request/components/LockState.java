package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
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
public class LockState {
  @Schema(description = "locked state of a table, example = false, true")
  @Builder.Default
  boolean locked = false;

  @Schema(description = "Customer-facing message describing the lock.", example = "")
  @Builder.Default
  String message = "Default";

  @Schema(
      description = "Structured lock reason. Omitted or null values default to LEGACY.",
      defaultValue = "LEGACY",
      nullable = true)
  @Builder.Default
  LockReason reason = LockReason.LEGACY;

  @Schema(
      description =
          "Authenticated principal recorded when the lock carries a structured reason such as "
              + "TIER3_AUTO_CLEANUP.")
  String lockOwner;

  @Schema(
      description =
          "Table UUID the lock pins, recorded when the lock carries a structured reason such as "
              + "TIER3_AUTO_CLEANUP.")
  String tableUUID;

  @Schema(
      description = "lock creation epoch time measured in UTC milliseconds for a table",
      example = "1651002318265")
  @Builder.Default
  long creationTime = System.currentTimeMillis();

  @Schema(
      description = "lock expiration time for a table is `n` days from creationTime",
      example = "3")
  @Builder.Default
  int expirationInDays = 0;

  public LockReason getReason() {
    return reason == null ? LockReason.LEGACY : reason;
  }
}
