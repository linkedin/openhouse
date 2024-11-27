package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.SnapshotRetention;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;

public class SnapshotRetentionPolicySpecValidator {

  private String failureMessage = "";
  private String errorField = "";

  protected boolean validate(SnapshotRetention snapshotRetention, TableUri tableUri) {
    if (snapshotRetention != null) {
      if (snapshotRetention.getGranularity() == null && snapshotRetention.getTimeCount() >= 0
          || snapshotRetention.getGranularity() != null && snapshotRetention.getTimeCount() < 0) {
        failureMessage =
            String.format(
                "Incorrect timeCount specified. snapshotRetention.timeCount must be defined together with snapshotRetention.granularity for table %s",
                tableUri);
        return false;
      }
      if (snapshotRetention.getTimeCount() > 0
          && snapshotRetention.getVersionCount() > 0
          && snapshotRetention.getLogicalOperator() == null) {
        failureMessage =
            String.format(
                "Must define logical operator to describe behavior in combined policies snapshotRetention.timeCount and snapshotRetention.versionCount for table %s",
                tableUri);
        return false;
      }
      if (!validateSnapshotRetentionMaximums(snapshotRetention)) {
        failureMessage =
            String.format("Snapshot retention for the table [%s] cannot exceed", tableUri);
        return false;
      }
    }
    return true;
  }

  protected boolean validateSnapshotRetentionMaximums(SnapshotRetention snapshotRetention) {
    int timeCount = snapshotRetention.getTimeCount();
    TimePartitionSpec.Granularity granularity = snapshotRetention.getGranularity();
    int versionCount = snapshotRetention.getVersionCount();
    boolean isGranularityValid =
        granularity.equals(TimePartitionSpec.Granularity.HOUR)
            || granularity.equals(TimePartitionSpec.Granularity.DAY);
    boolean isTimeCountValid =
        !(timeCount >= 3 && granularity.equals(TimePartitionSpec.Granularity.DAY)
            || timeCount >= 72 && granularity.equals(TimePartitionSpec.Granularity.HOUR));
    boolean isVersionCountValid = versionCount >= 100;
    return isGranularityValid && isTimeCountValid && isVersionCountValid;
  }

  public String getMessage() {
    return failureMessage;
  }

  public String getField() {
    return errorField;
  }
}
