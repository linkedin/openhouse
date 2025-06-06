package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Class to manage submitted/running job info and used for multi mode (submit and poll) operation
 * only.
 */
public class JobInfoManager extends BaseDataManager<JobInfo> {
  protected Set<String> runningJobs = Collections.synchronizedSet(new HashSet<>());

  public JobInfoManager(JobConf.JobTypeEnum jobType) {
    super(jobType);
  }

  public void addData(JobInfo jobInfo) throws InterruptedException {
    super.addData(jobInfo);
    runningJobs.add(jobInfo.getJobId());
  }

  /**
   * Provides currently running job count
   *
   * @return int
   */
  public int runningJobsCount() {
    return runningJobs.size();
  }

  /**
   * Remove the specified job id from the running jobs
   *
   * @param jobId
   */
  public void moveJobToCompletedStage(String jobId) {
    if (jobId != null && runningJobs.contains(jobId)) {
      runningJobs.remove(jobId);
    }
  }

  /** Resets all the fields */
  public void resetAll() {
    super.resetAll();
    runningJobs.clear();
  }
}
