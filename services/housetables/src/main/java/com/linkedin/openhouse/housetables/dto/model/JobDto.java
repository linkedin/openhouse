package com.linkedin.openhouse.housetables.dto.model;

import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class JobDto {

  String jobId;

  String state;

  String version;

  String jobName;

  String clusterId;

  Long creationTimeMs;

  Long startTimeMs;

  Long finishTimeMs;

  Long lastUpdateTimeMs;

  String jobConf;

  Long heartbeatTimeMs;

  String executionId;

  /**
   * Compare if a given {@link JobDto} matches the current one if all non-null fields are equal.
   *
   * @param jobDto the object to be examined.
   * @return true if matched.
   */
  public boolean match(JobDto jobDto) {
    return Utilities.fieldMatch(this.jobId, jobDto.jobId)
        && Utilities.fieldMatch(this.state, jobDto.state)
        && Utilities.fieldMatch(this.version, jobDto.version)
        && Utilities.fieldMatch(this.jobName, jobDto.jobName)
        && Utilities.fieldMatch(this.clusterId, jobDto.clusterId)
        && Utilities.fieldMatch(this.creationTimeMs, jobDto.creationTimeMs)
        && Utilities.fieldMatch(this.startTimeMs, jobDto.startTimeMs)
        && Utilities.fieldMatch(this.finishTimeMs, jobDto.finishTimeMs)
        && Utilities.fieldMatch(this.lastUpdateTimeMs, jobDto.lastUpdateTimeMs)
        && Utilities.fieldMatch(this.jobConf, jobDto.jobConf)
        && Utilities.fieldMatch(this.heartbeatTimeMs, jobDto.heartbeatTimeMs)
        && Utilities.fieldMatch(this.executionId, jobDto.executionId);
  }
}
