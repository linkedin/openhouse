package com.linkedin.openhouse.jobs.model;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JobDtoPrimaryKey implements Serializable, Comparable<JobDtoPrimaryKey> {
  private String jobId;

  @Override
  public int compareTo(JobDtoPrimaryKey o) {
    return jobId.compareTo(o.jobId);
  }
}
