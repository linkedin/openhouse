package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.model.JobDto;

/** Service interface for implementing /jobs endpoint. */
public interface JobsService {
  /**
   * Given a jobId, return a {@link JobDto}
   *
   * @param jobId a unique job identifier
   * @return JobDto Job object with given jobId
   */
  JobDto get(String jobId);

  /**
   * Given a {@link CreateJobRequestBody}, create a new Job resource.
   *
   * @param createJobRequestBody request body to create Job object from
   * @return Saved Job object {@link JobDto}
   */
  JobDto create(CreateJobRequestBody createJobRequestBody);

  /**
   * Cancel job with given jobId. Does nothing if it's in a terminal state (failed, succeeded,
   * cancelled).
   *
   * @param jobId unique job identifier
   */
  void cancel(String jobId);
}
