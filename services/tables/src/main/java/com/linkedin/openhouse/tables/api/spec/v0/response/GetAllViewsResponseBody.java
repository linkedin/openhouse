package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import org.springframework.data.domain.Page;

/** Paginated view identifiers and page metadata. */
@Builder
@Value
public class GetAllViewsResponseBody {

  @Schema(description = "Page of View objects in a database", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Page<GetViewResponseBody> pageResults;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
