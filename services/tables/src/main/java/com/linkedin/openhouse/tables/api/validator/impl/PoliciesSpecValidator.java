package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Schedule;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.DefaultColumnPattern;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;
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

      // invalid cases regarding the integrity of retention object.
      if (!validateGranularityWithPattern(policies.getRetention())) {
        failureMessage =
            String.format(
                "Provided Retention Granularity[%s] is not supported with default pattern. "
                    + "Please define pattern in retention config or use one of supported granularity: %s",
                policies.getRetention().getGranularity().name(),
                Arrays.toString(DefaultColumnPattern.values()));
        return false;
      }
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
      if (!validateReplicationPolicy(policies.getReplication())) {
        failureMessage =
            String.format(
                "For table with non-null replication policy, a valid replication schedule %s is mandatory",
                policies.getReplication());
        return false;
      }
    }
    return true;
  }

  private boolean validateReplicationPolicy(Replication replication) {
    // implement replication config validation
    if (replication != null) {
      for (Schedule schedule : replication.getSchedules()) {
        Map<String, String> config = schedule.getConfig();
        for (String key : config.keySet()) {
          try {
            CronExpression.parse(config.get(key));
          } catch (IllegalArgumentException e) {
            return false;
          }
        }
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

  /** validate the granularity provided is supported by default {@link DefaultColumnPattern} */
  protected boolean validateGranularityWithPattern(Retention retention) {
    if (retention.getColumnPattern() != null
        && retention.getColumnPattern().getPattern().isEmpty()) {
      try {
        DefaultColumnPattern.valueOf(retention.getGranularity().name());
      } catch (IllegalArgumentException e) {
        log.warn(
            "Retention Granularity {} is not supported with default retention column pattern",
            retention.getGranularity().name());
        return false;
      }
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
