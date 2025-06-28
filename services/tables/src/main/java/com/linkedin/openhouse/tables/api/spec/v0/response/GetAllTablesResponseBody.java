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
public class GetAllTablesResponseBody {

  @Schema(description = "List of Table objects in a database", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<GetTableResponseBody> results;

  @Schema(description = "Page of Table objects in a database", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Page<GetTableResponseBody> pageResults;

  /**
   * TODO: spec out the pagination part, something like _metadata: { "offset": 20, "limit": 10,
   * "total": 3465, } links: { "next": "", "self": "", "prev": "" }
   */
  public String toJson() {
    return new Gson().toJson(this);
  }
}
