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
public class UpdateLockedStateRequestBody {
  @Schema(description = "locked state to be updated for table.")
  @NotEmpty(message = "locked cannot be cannot be empty")
  @NotNull(message = "locked State cannot be null")
  boolean locked;

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
