package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HistoryPolicySpecValidator extends PolicySpecValidator {

  // History retention configuration constants
  private static final int MIN_VERSIONS = 2;
  private static final int MAX_VERSIONS = 900;
  private static final int MIN_DAYS_RETENTION = 1;
  private static final int MAX_DAYS_RETENTION = 3;
  private static final int MIN_HOURS_RETENTION = MIN_DAYS_RETENTION * 24; // 1 day in hours
  private static final int MAX_HOURS_RETENTION = MAX_DAYS_RETENTION * 24; // 3 days in hours

  public boolean validate(
      CreateUpdateTableRequestBody createUpdateTableRequestBody, TableUri tableUri) {
    History history = createUpdateTableRequestBody.getPolicies().getHistory();
    if (history != null) {
      if (history.getMaxAge() <= 0 && history.getVersions() <= 0) {
        failureMessage =
            String.format(
                "Must define either a time based retention or count based retention for snapshots in table %s",
                tableUri);
        return false;
      }

      if (history.getGranularity() == null && history.getMaxAge() > 0
          || history.getGranularity() != null && history.getMaxAge() <= 0) {
        failureMessage =
            String.format(
                "Incorrect maxAge specified. history.maxAge must be defined together with history.granularity for table %s",
                tableUri);
        return false;
      }

      if (!validateHistoryConfigMaxAgeWithinBounds(history)) {
        failureMessage =
            String.format(
                "History for the table [%s] max age must be between 1 to 3 days", tableUri);
        return false;
      }

      if (!validateHistoryConfigVersionsWithinBounds(history)) {
        failureMessage =
            String.format("History for the table [%s] must be between 2 to 100 versions", tableUri);
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that the amount of time to retain history of table snapshots is between 1 and 3 days
   *
   * @param history
   * @return
   */
  protected boolean validateHistoryConfigMaxAgeWithinBounds(History history) {
    int maxAge = history.getMaxAge();
    TimePartitionSpec.Granularity granularity = history.getGranularity();
    // if maxAge is 0 then consider it undefined and refer to default for snapshot expiration
    if (maxAge == 0) {
      return true;
    }

    if (granularity.equals(TimePartitionSpec.Granularity.HOUR)) {
      return maxAge >= MIN_HOURS_RETENTION && maxAge <= MAX_HOURS_RETENTION;
    } else if (granularity.equals(TimePartitionSpec.Granularity.DAY)) {
      return maxAge >= MIN_DAYS_RETENTION && maxAge <= MAX_DAYS_RETENTION;
    }

    return false;
  }

  /*
   * Validate that the number of versions to retain history of table snapshots is between MIN_VERSIONS and MAX_VERSIONS
   * We want at least MIN_VERSIONS so that users can always rollback to at least 1 version before a commit
   */
  protected boolean validateHistoryConfigVersionsWithinBounds(History history) {
    if (history.getVersions()
        == 0) { // versions is 0 then consider it undefined and refer to default for snapshot
      // expiration
      return true;
    }
    int versions = history.getVersions();
    return versions >= MIN_VERSIONS && versions <= MAX_VERSIONS;
  }
}
