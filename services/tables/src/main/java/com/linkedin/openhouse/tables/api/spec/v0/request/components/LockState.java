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
  boolean isLocked = false;

  @Schema(description = "message for locking", example = "")
  String message;
}
