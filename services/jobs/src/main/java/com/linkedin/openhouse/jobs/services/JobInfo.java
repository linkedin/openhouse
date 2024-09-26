package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.common.JobState;
import java.util.Map;

/** Interface for access to job information. */
public interface JobInfo {
  /**
   * @return state of the job {@link JobState}
   */
  JobState getState();

  /**
   * @return id of the job in the engine API, e.g. sessionId in Livy.
   */
  String getExecutionId();

  /**
   * @return id specific to cluster where the app is deployed, e.g. YARN application id.
   */
  String getAppId();

  /**
   * @return properties specific to cluster where the app is deployed, e.g. YARN app urls.
   */
  Map<String, String> getAppInfo();
}
