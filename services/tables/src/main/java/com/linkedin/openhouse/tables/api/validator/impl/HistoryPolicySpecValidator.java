package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.History;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HistoryPolicySpecValidator {

  private String failureMessage = "";
  private String errorField = "";

  protected boolean validate(History history, TableUri tableUri) {
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

      if (!validateHistoryConfigMaximums(history)) {
        failureMessage =
            String.format(
                "History for the table [%s] cannot exceed 3 days or 100 versions maximum",
                tableUri);
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that the amount of time to retain snapshots does not exceed either 3 days or 100
   * versions
   *
   * @param history
   * @return
   */
  protected boolean validateHistoryConfigMaximums(History history) {
    int maxAge = history.getMaxAge();
    TimePartitionSpec.Granularity granularity = history.getGranularity();
    int versions = history.getVersions();
    boolean isGranularityValid =
        granularity == null
            || granularity.equals(TimePartitionSpec.Granularity.HOUR)
            || granularity.equals(TimePartitionSpec.Granularity.DAY);

    boolean isMaxAgeValid =
        !(maxAge > 3 && granularity.equals(TimePartitionSpec.Granularity.DAY)
            || maxAge > 72 && granularity.equals(TimePartitionSpec.Granularity.HOUR));
    boolean isversionsValid = versions <= 100;
    return isGranularityValid && isMaxAgeValid && isversionsValid;
  }

  public String getMessage() {
    return failureMessage;
  }

  public String getField() {
    return errorField;
  }
}
