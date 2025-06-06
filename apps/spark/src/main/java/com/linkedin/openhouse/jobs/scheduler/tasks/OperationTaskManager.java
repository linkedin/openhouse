package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.model.JobConf;

/**
 * Class to manage the operation tasks which are generated for single mode or multi mode (submit &
 * poll) submission
 */
public class OperationTaskManager extends BaseDataManager<OperationTask<?>> {
  public OperationTaskManager(JobConf.JobTypeEnum jobType) {
    super(jobType);
  }
}
