package com.linkedin.openhouse.housetables.api.spec.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;

// TODO: Support pagination
@Builder
public class GetAllEntityResponseBody<T> {

  @Schema(description = "List of user table objects in House table", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @SuppressFBWarnings(
      value = "URF_UNREAD_FIELD",
      justification = "Value referenced in generated client code.")
  private List<T> results;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
