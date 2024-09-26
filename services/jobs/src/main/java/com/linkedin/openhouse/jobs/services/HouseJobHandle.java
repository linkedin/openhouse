package com.linkedin.openhouse.jobs.services;

/**
 * Interface for House Job handle that provides access to a specific job state and management
 * methods.
 */
public interface HouseJobHandle {
  /** Issues cancel request to the engine API to terminate the job. */
  void cancel();

  /**
   * @return job info {@link com.linkedin.openhouse.jobs.services.JobInfo}
   */
  JobInfo getInfo();
}
