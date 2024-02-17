package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * PoliciesSpecValidator is a custom validator to validate the input values for period in retention
 * policy. This custom validator can be used to add validators for fields in policies
 */
@Component
@Slf4j
public class PoliciesSpecValidator {

  private String failureMessage = "";

  private String errorField = "";
  /**
   * Invalid cases for retention object 0. retention column not found in the schema object. 1.
   * retention column pattern containing invalid characters. 2. missing retention column pattern in
   * provided retention object when table is not time-partitioned. (Otherwise such retention isn't
   * useful) 3(1). when table is time-partitioned: providing retention column type is invalid. 3(2)
   * when table is time-partitioned: Granularity mismatch with retention column is invalid.
   *
   * @param policies {@link Policies} Policies object that needs to be validated and set. null
   *     policy object is accepted.
   * @param timePartitioning {@link TimePartitionSpec} TimePartitionSpec containing the granularity
   *     against which policies.retention.granularity is validated
   * @param schema {@link String} Raw schema representation deserialized from wire.
   * @return Boolean validity of constraint
   */
  public boolean validate(
      Policies policies, TimePartitionSpec timePartitioning, TableUri tableUri, String schema) {

    if (policies != null && policies.getRetention() != null) {
      // Two invalid case for timePartitioned table
      if (timePartitioning != null) {
        if (policies.getRetention().getColumnPattern() != null) {
          failureMessage =
              String.format(
                  "You can only specify retention column pattern on non-timestampPartitioned table (table[%s] is time-partitioned by[%s])",
                  tableUri, timePartitioning.getColumnName());
          return false;
        }
        if (!policies.getRetention().getGranularity().equals(timePartitioning.getGranularity())) {
          failureMessage =
              String.format(
                  "invalid policies retention granularity format for table %s. Policies granularity must be equal to or lesser than"
                      + " time partition spec granularity",
                  tableUri);
          errorField = "retention";
          return false;
        }
      }

      // Two invalid case regarding the integrity of retention object.
      if (!validatePatternIfPresent(policies.getRetention(), tableUri, schema)) {
        failureMessage =
            String.format(
                "Provided pattern[%s] is not recognizable by OpenHouse for the table[%s]; Also please make sure the declared column is part of table schema.",
                policies.getRetention().getColumnPattern(), tableUri);
        return false;
      }
      if (timePartitioning == null && policies.getRetention().getColumnPattern() == null) {
        failureMessage =
            String.format(
                "For non timestamp-partitioned table %s, column pattern in retention policy is mandatory",
                tableUri);
        return false;
      }
    }
    return true;
  }

  /**
   * Validate the pattern provided by users are legit pattern that complies with {@link
   * DateTimeFormatter} symbols. Also, the provided column name needs to be part of schema.
   */
  public boolean validatePatternIfPresent(Retention retention, TableUri tableUri, String schema) {
    if (retention.getColumnPattern() != null) {
      if (retention.getColumnPattern().getColumnName() != null
          && !columnExists(
              getSchemaFromSchemaJson(schema), retention.getColumnPattern().getColumnName())) {
        return false;
      }
      return isPatternValid(retention.getColumnPattern().getPattern(), tableUri);
    }

    return true;
  }

  protected boolean isPatternValid(String pattern, TableUri tableUri) {
    try {
      DateTimeFormatter.ofPattern(pattern);
    } catch (IllegalArgumentException illegalArgumentException) {
      log.warn(
          "The pattern provided {} cannot be parsed correctly for the table {}", pattern, tableUri);
      return false;
    }

    return true;
  }

  public String getField() {
    return errorField;
  }

  public String getMessage() {
    return failureMessage;
  }
}
