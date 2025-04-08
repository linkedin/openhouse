package com.linkedin.openhouse.jobs.model;

import com.linkedin.openhouse.common.JobState;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** Data Model for persisting Job object in OpenHouseCatalog. */
@Entity
@IdClass(JobDtoPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public class JobDto {
  // all timestamps are in milliseconds
  @Id private String jobId;

  private String jobName;
  private String clusterId;

  @Enumerated private JobState state;

  private long creationTimeMs;
  private long startTimeMs;
  private long finishTimeMs;
  private long lastUpdateTimeMs;

  @Convert(converter = JobConfConverter.class)
  @ToString.Exclude
  private JobConf jobConf;

  // internal attributes for jobs operation
  // heartbeat time from a job to monitor liveness
  private long heartbeatTimeMs;
  // job execution engine job identifier
  private String executionId;
  @Enumerated private EngineType engineType;
}
