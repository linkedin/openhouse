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

  @Schema(description = "message for locking", example = "")
  @Builder.Default
  String message = "Default";

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
}
