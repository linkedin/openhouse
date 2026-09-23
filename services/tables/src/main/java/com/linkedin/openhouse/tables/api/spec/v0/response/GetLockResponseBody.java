package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class GetLockResponseBody {
  @Schema(description = "Active lock, or null when the table is not locked.", nullable = true)
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private LockState lockState;
}
