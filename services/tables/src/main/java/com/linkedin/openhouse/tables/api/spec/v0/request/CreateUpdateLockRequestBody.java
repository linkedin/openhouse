package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
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

  @Schema(description = "Optional customer-facing message describing the lock.")
  String message;

  @Schema(
      description = "Structured lock reason. Omitted or null values default to LEGACY.",
      defaultValue = "LEGACY",
      nullable = true)
  @Builder.Default
  LockReason reason = LockReason.LEGACY;

  @Schema(
      description =
          "Table UUID the lock pins. Required when reason names a structured reason such as "
              + "TIER3_AUTO_CLEANUP.")
  String expectedTableUUID;

  @Schema(
      description = "lock creation epoch time measured in UTC milliseconds for a table",
      example = "1651002318265")
  @Builder.Default
  long creationTime = System.currentTimeMillis();

  @Schema(
      description = "lock expiration time for a table is `n` days from creationTime",
      example = "3")
  int expirationInDays = 0;

  public LockReason getReason() {
    return reason == null ? LockReason.LEGACY : reason;
  }

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
