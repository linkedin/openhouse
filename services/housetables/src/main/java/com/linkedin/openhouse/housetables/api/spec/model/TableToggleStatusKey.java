package com.linkedin.openhouse.housetables.api.spec.model;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import lombok.Builder;
import lombok.Value;

/** Key to query feature-toggle status of a table. */
@Builder
@Value
public class TableToggleStatusKey {
  @Schema(
      description =
          "Unique Resource identifier for the Database containing the Table. Together with tableID"
              + " they form a composite primary key for a user table.",
      example = "my_database")
  @JsonProperty(value = "databaseId")
  @NotEmpty(message = "databaseId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  String databaseId;

  @Schema(
      description = "Unique Resource identifier for a table within a Database.",
      example = "my_table")
  @JsonProperty(value = "tableId")
  @NotEmpty(message = "tableId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  String tableId;

  @Schema(
      description = "Unique Resource identifier for a feature within OpenHouse Service",
      example = "dummy")
  @JsonProperty(value = "featureId")
  @NotEmpty(message = "featureId cannot be empty")
  String featureId;
}
