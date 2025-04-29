package com.linkedin.openhouse.housetables.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Version;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@IdClass(JobRowPrimaryKey.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JobRow {

  @Id String jobId;

  String state;

  @Version Long version;

  String jobName;

  String clusterId;

  Long creationTimeMs;

  Long startTimeMs;

  Long finishTimeMs;

  Long lastUpdateTimeMs;

  String jobConf;

  Long heartbeatTimeMs;

  String executionId;

  String engineType;
}
