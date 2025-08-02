package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import org.springframework.data.domain.Page;

@Builder
@Value
public class GetAllSoftDeletedTablesResponseBody {

  @Schema(
      description = "Page of soft deleted tables matching the filters by database",
      example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Page<GetSoftDeletedTableResponseBody> pageResults;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
