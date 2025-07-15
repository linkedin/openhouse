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

/**
 * The row type for the House table storing user tables. {@link JsonProperty} annotation is needed
 * when a field is declared as private to instruct jackson library initialize and deserialize the
 * model object without no-arg constructor provided.
 */
@Builder(toBuilder = true)
@Value
public class UserTable {
  @Schema(
      description = "Unique Resource identifier for a table within a Database.",
      example = "my_table")
  @JsonProperty(value = "tableId")
  @NotEmpty(message = "tableId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String tableId;

  @Schema(
      description =
          "Unique Resource identifier for the Database containing the Table. Together with tableID"
              + " they form a composite primary key for a user table.",
      example = "my_database")
  @JsonProperty(value = "databaseId")
  @NotEmpty(message = "databaseId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String databaseId;

  @Schema(
      description = "Current Version of the user table. New record should have 'INTITAL_VERISON'",
      example = "")
  @JsonProperty(value = "tableVersion")
  private String tableVersion;

  @Schema(
      description = "Full URI for the file manifesting the newest version of a user table.",
      example = "")
  @JsonProperty(value = "metadataLocation")
  @NotEmpty(message = "metadataLocation cannot be empty")
  private String metadataLocation;

  @Schema(
      description = "Storage type to be used for the table.",
      example = "hdfs",
      defaultValue = "hdfs")
  @JsonProperty(value = "storageType")
  private String storageType;

  @Schema(description = "Creation time of the table.", example = "1651002318265")
  @JsonProperty(value = "creationTime")
  private Long creationTime;

  @Schema(
      description =
          "Timestamp in milliseconds when the table was soft-deleted. "
              + "If null, then the table is active",
      example = "1751907524",
      defaultValue = "null")
  private Long deletedAtMs;

  @Schema(
      description =
          "Unix timestamp in milliseconds indicating when the table will be deleted permanently by another process. "
              + "If the current time is past the TTL, then the table could be arbitrarily deleted at any time.",
      example = "1751907524",
      defaultValue = "null")
  private Long purgeAfterMs;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
