package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.DefaultColumnPattern;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * RetentionPolicySpecValidator is a custom validator to validate the input values for period in
 * retention policy.
 */
@Component
@Slf4j
public class RetentionPolicySpecValidator extends PolicySpecValidator {

  /**
   * Invalid cases for retention object 0. retention column not found in the schema object. 1.
   * retention column pattern containing invalid characters. 2. missing retention column pattern in
   * provided retention object when table is not time-partitioned. (Otherwise such retention isn't
   * useful) 3(1). when table is time-partitioned: providing retention column type is invalid. 3(2)
   * when table is time-partitioned: Granularity mismatch with retention column is invalid.
   *
   * @param createUpdateTableRequestBody {@link CreateUpdateTableRequestBody} API request body for
   *     creation and updating tables
   * @return Boolean validity of constraint
   */
  @Override
  public boolean validate(
      CreateUpdateTableRequestBody createUpdateTableRequestBody, TableUri tableUri) {
    Retention retention = createUpdateTableRequestBody.getPolicies().getRetention();
    TimePartitionSpec timePartitioning = createUpdateTableRequestBody.getTimePartitioning();
    String schema = createUpdateTableRequestBody.getSchema();

    if (retention != null) {
      // Two invalid case for timePartitioned table
      if (timePartitioning != null) {
        if (retention.getColumnPattern() != null) {
          failureMessage =
              String.format(
                  "You can only specify retention column pattern on non-timestampPartitioned table (table[%s] is time-partitioned by[%s])",
                  tableUri, timePartitioning.getColumnName());
          return false;
        }
        if (!retention.getGranularity().equals(timePartitioning.getGranularity())) {
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
      if (!validateGranularityWithPattern(retention)) {
        failureMessage =
            String.format(
                "Provided Retention Granularity[%s] is not supported with default pattern. "
                    + "Please define pattern in retention config or use one of supported granularity: %s",
                retention.getGranularity().name(), Arrays.toString(DefaultColumnPattern.values()));
        return false;
      }
      if (!validatePatternIfPresent(retention, tableUri, schema)) {
        failureMessage =
            String.format(
                "Provided pattern[%s] is not recognizable by OpenHouse for the table[%s]; Also please make sure the declared column is part of table schema.",
                retention.getColumnPattern(), tableUri);
        return false;
      }
      if (timePartitioning == null && retention.getColumnPattern() == null) {
        failureMessage =
            String.format(
                "For non timestamp-partitioned table %s, column pattern in retention policy is mandatory",
                tableUri);
        return false;
      }
      if (!validateTimeZoneIfPresent(retention)) {
        failureMessage =
            String.format(
                "Provided retention time zone[%s] is not a valid IANA zone id or fixed offset for the table[%s]",
                retention.getTimeZone(), tableUri);
        errorField = "retention";
        return false;
      }
      if (!validateTimeZoneScope(retention, timePartitioning)) {
        failureMessage =
            String.format(
                "Retention time zone[%s] is only supported on a string retention column whose pattern"
                    + " has no zone field; it is not allowed on the time-partitioned (native"
                    + " timestamp) table[%s] or on a column pattern that already encodes a zone",
                retention.getTimeZone(), tableUri);
        errorField = "retention";
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

  /**
   * Validate that the retention time zone, when present, is a value {@link ZoneId} can resolve: an
   * IANA zone id such as {@code America/Los_Angeles} or a fixed offset such as {@code +05:30}. An
   * absent or empty time zone means UTC and is valid.
   */
  protected boolean validateTimeZoneIfPresent(Retention retention) {
    String timeZone = retention.getTimeZone();
    if (timeZone == null || timeZone.isEmpty()) {
      return true;
    }
    try {
      ZoneId.of(timeZone);
    } catch (DateTimeException dateTimeException) {
      log.warn("The retention time zone {} cannot be resolved to a valid zone", timeZone);
      return false;
    }
    return true;
  }

  /**
   * A retention time zone is meaningful only for a string retention column whose pattern carries no
   * zone of its own. Native timestamp columns store UTC instants, and a pattern that already
   * formats a zone would double count, so both reject a declared zone. An absent or empty zone is
   * always in scope.
   */
  protected boolean validateTimeZoneScope(Retention retention, TimePartitionSpec timePartitioning) {
    String timeZone = retention.getTimeZone();
    if (timeZone == null || timeZone.isEmpty()) {
      return true;
    }
    if (timePartitioning != null) {
      return false;
    }
    return retention.getColumnPattern() == null
        || retention.getColumnPattern().getPattern() == null
        || !patternEncodesZone(retention.getColumnPattern().getPattern());
  }

  /** True when the pattern contains a DateTimeFormatter zone or offset field outside a literal. */
  protected boolean patternEncodesZone(String pattern) {
    String withoutLiterals = pattern.replaceAll("'[^']*'", "");
    return withoutLiterals.chars().anyMatch(c -> "VzOXxZ".indexOf(c) >= 0);
  }
}
