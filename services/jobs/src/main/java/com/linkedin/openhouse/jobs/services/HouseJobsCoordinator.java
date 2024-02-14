package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.jobs.config.JobLaunchConf;

/**
 * Interface for House Jobs coordinator, responsible for jobs launching, tracking and cancellation
 * via {@link HouseJobHandle}.
 */
public interface HouseJobsCoordinator {
  /**
   * Submits a job via Spark engine API of choice.
   *
   * @param conf - job launch config {@link JobLaunchConf}
   * @return handle object {@link HouseJobHandle} to manage this specific job
   */
  HouseJobHandle submit(JobLaunchConf conf);

  /**
   * Given an id of the job engine API, creates a job handle.
   *
   * @param executionId - id of the job in the engine API, e.g. sessionId in Livy
   * @return handle object {@link HouseJobHandle} to manage this specific job
   */
  HouseJobHandle obtainHandle(String executionId);
}
