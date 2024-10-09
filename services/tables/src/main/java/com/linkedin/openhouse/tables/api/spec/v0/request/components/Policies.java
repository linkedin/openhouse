package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import javax.validation.Valid;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Policies is the entity for holding Policy specification for a table in request body. Using this
 * class to deserialize policies Json from request
 */
@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Policies {

  @Schema(
      description =
          "Retention as required in /tables API request. The column holds the retention part or Policies.",
      example = "{retention:{count:3, granularity: 'day'}}")
  @Valid
  Retention retention;

  @Schema(
      description =
          "Whether data sharing needs to enabled for the table in /tables API request. Sharing is disabled "
              + "by default",
      example = "{sharingEnabled = 'true'}")
  boolean sharingEnabled;

  // TODO: Add validation for if the column exists on the table
  @Schema(
      description = "Policy tags applied to columns in /tables API request.",
      example = "{'colName': [PII, HC]}")
  @Valid
  Map<String, PolicyTag> columnTags;

  @Schema(
      description =
          "Replication as required in /tables API request. This column holds the replication spec config.",
      example = "{destination: a, interval: 12hour}")
  @Valid
  Replication replication;
}
