package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Effective column-level read entitlements of the authenticated caller on a table.
 *
 * <p>The catalog resolves the caller's grants against the policy tags carried by the table's
 * columns and returns the outcome, so that query engines never have to reason about roles
 * themselves.
 */
@Builder
@Value
public class GetColumnEntitlementsResponseBody {
  @Schema(
      description = "Policy tags present on the table that the caller is entitled to read",
      example = "[\"PII\"]")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<String> grantedTags;

  @Schema(
      description =
          "Columns the caller is not entitled to read. A tagged column is restricted unless the "
              + "caller holds a grant for every tag on that column.",
      example = "[\"ssn\"]")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<String> restrictedColumns;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
