package com.linkedin.openhouse.tables.api.spec.v0.request;

import com.google.gson.GsonBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Filter for the table search API. Every field is optional; a request that omits the body entirely,
 * or supplies one with no fields set, searches the database without any additional filtering.
 *
 * <p>Modelled as a request body rather than as query parameters so that later filters can be added
 * as new fields without changing the signature of the operation, which keeps generated clients both
 * source- and binary-compatible.
 */
@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class SearchTablesRequestBody {

  @Schema(
      description =
          "Optional table name to filter the search by. Matched exactly: pattern and wildcard "
              + "matching are not supported, and a value containing characters outside "
              + "[a-zA-Z0-9_] is rejected. When set, the result holds at most one table.",
      example = "my_table",
      nullable = true)
  String tableId;

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
