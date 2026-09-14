package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * One page of view identifiers and the token that continues the listing.
 *
 * <p>{@code results} is always present, including as an empty array. {@code nextPageToken} is
 * omitted once the listing is complete: its absence, rather than the number of results, is what
 * tells a client to stop. Jackson and the Gson helper agree on that omission.
 */
@Builder
@Value
public class GetAllViewsResponseBody {

  @Schema(
      description = "View objects in a database, at most as many as the requested size",
      required = true,
      example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @NonNull
  private List<GetViewResponseBody> results;

  @Schema(
      description =
          "Opaque token that returns the next page when sent back as pageToken. Absent once the "
              + "listing is complete.",
      example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String nextPageToken;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
