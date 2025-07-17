package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.springframework.data.domain.Page;

@Builder
@Value
public class GetAllDatabasesResponseBody {
  @Schema(description = "List of Database objects", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<GetDatabaseResponseBody> results;

  @Schema(description = "Page of Database objects", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Page<GetDatabaseResponseBody> pageResults;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
