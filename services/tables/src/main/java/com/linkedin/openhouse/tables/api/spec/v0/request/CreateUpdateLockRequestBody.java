package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.GsonBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
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
public class CreateUpdateLockRequestBody {
  @Schema(description = "locked state to be created/updated for table.")
  @NotEmpty(message = "locked cannot be empty")
  @NotNull(message = "locked value cannot be null")
  boolean locked;

  @Schema(description = "reason for creating/updating the lock on table")
  String message;

  @Schema(
      description = "lock creation epoch time measured in UTC milliseconds for a table",
      example = "1651002318265")
  @Builder.Default
  long creationTime = System.currentTimeMillis();

  @Schema(
      description = "lock expiration time for a table is `n` days from creationTime",
      example = "3")
  int expirationInDays = 0;

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
