package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.schema.IcebergSchemaHelper.*;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Retention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.DefaultColumnPattern;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import lombok.Value;
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
    Optional<Violation> violation =
        findViolation(
            createUpdateTableRequestBody.getPolicies().getRetention(),
            createUpdateTableRequestBody.getTimePartitioning(),
            createUpdateTableRequestBody.getSchema(),
            tableUri);
    if (violation.isPresent()) {
      failureMessage = violation.get().getMessage();
      errorField = violation.get().getField();
      return false;
    }
    return true;
  }

  /**
   * Stateless equivalent of {@link #validate(CreateUpdateTableRequestBody, TableUri)} that checks a
   * retention policy for compatibility with the given schema and time-partitioning spec. Callers
   * that do not operate on a {@link CreateUpdateTableRequestBody}, such as the REPLACE TABLE path
   * where retention is carried over from the existing table, should use this method.
   *
   * @param retention retention policy to check, a null retention is always compatible
   * @param timePartitioning time-partitioning spec of the table, null when not time-partitioned
   * @param schema Iceberg schema of the table in its JSON representation
   * @param tableUri identifier of the table used in failure messages
   * @return the {@link Violation} found, or {@link Optional#empty()} when retention is compatible
   */
  public Optional<Violation> findViolation(
      Retention retention, TimePartitionSpec timePartitioning, String schema, TableUri tableUri) {
    if (retention == null) {
      return Optional.empty();
    }

    // Two invalid case for timePartitioned table
    if (timePartitioning != null) {
      if (retention.getColumnPattern() != null) {
        return Optional.of(
            new Violation(
                "",
                String.format(
                    "You can only specify retention column pattern on non-timestampPartitioned table (table[%s] is time-partitioned by[%s])",
                    tableUri, timePartitioning.getColumnName())));
      }
      if (!retention.getGranularity().equals(timePartitioning.getGranularity())) {
        return Optional.of(
            new Violation(
                "retention",
                String.format(
                    "invalid policies retention granularity format for table %s. Policies granularity must be equal to or lesser than"
                        + " time partition spec granularity",
                    tableUri)));
      }
    }

    // invalid cases regarding the integrity of retention object.
    if (!validateGranularityWithPattern(retention)) {
      return Optional.of(
          new Violation(
              "",
              String.format(
                  "Provided Retention Granularity[%s] is not supported with default pattern. "
                      + "Please define pattern in retention config or use one of supported granularity: %s",
                  retention.getGranularity().name(),
                  Arrays.toString(DefaultColumnPattern.values()))));
    }
    if (!validatePatternIfPresent(retention, tableUri, schema)) {
      return Optional.of(
          new Violation(
              "",
              String.format(
                  "Provided pattern[%s] is not recognizable by OpenHouse for the table[%s]; Also please make sure the declared column is part of table schema.",
                  retention.getColumnPattern(), tableUri)));
    }
    if (timePartitioning == null && retention.getColumnPattern() == null) {
      return Optional.of(
          new Violation(
              "",
              String.format(
                  "For non timestamp-partitioned table %s, column pattern in retention policy is mandatory",
                  tableUri)));
    }

    return Optional.empty();
  }

  /** Immutable description of an incompatible retention policy. */
  @Value
  public static class Violation {
    String field;
    String message;
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
}
