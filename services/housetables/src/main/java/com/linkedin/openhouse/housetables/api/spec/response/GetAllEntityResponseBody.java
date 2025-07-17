package com.linkedin.openhouse.housetables.api.spec.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import org.springframework.data.domain.Page;

// TODO: Support pagination
@Builder
public class GetAllEntityResponseBody<T> {

  @Schema(description = "List of user table objects in House table", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @SuppressFBWarnings(
      value = "URF_UNREAD_FIELD",
      justification = "Value referenced in generated client code.")
  private List<T> results;

  @Schema(description = "Page of user table objects in House table", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @SuppressFBWarnings(
      value = "URF_UNREAD_FIELD",
      justification = "Value referenced in generated client code.")
  private Page<T> pageResults;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
