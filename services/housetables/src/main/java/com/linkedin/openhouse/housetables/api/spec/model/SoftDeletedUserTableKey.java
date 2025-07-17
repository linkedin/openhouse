package com.linkedin.openhouse.housetables.api.spec.model;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_ERROR_MSG;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import lombok.Builder;
import lombok.Value;

/** The key type for the House table storing user soft deleted tables. */
@Builder
@Value
public class SoftDeletedUserTableKey {
  @Schema(
      description = "Unique Resource identifier for a table within a Database.",
      example = "my_table")
  @JsonProperty(value = "tableId")
  @NotEmpty(message = "tableId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String tableId;

  @Schema(
      description = "Unique Resource identifier for the Database containing the Table.",
      example = "my_database")
  @JsonProperty(value = "databaseId")
  @NotEmpty(message = "databaseId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String databaseId;

  @Schema(
      description =
          "Delete time in unix epoch milliseconds. Needed as part of primary key to support multiple versions of a deleted table",
      example = "1651002318265")
  @JsonProperty(value = "deletedAtMs")
  @NotEmpty(message = "deletedAtMs cannot be empty")
  private long deletedAtMs;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
