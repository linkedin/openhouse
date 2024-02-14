package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import java.util.List;
import org.springframework.data.util.Pair;

/** Service Interface for Implementing /hts/jobs endpoint. */
public interface JobsService {
  /**
   * @param jobId primary key
   * @return {@link JobDto}. Avoid using {@link Job} directly for decoupling between service and
   *     transport layer.
   */
  JobDto getJob(String jobId);

  /**
   * Given a partially filled {@link Job} object, prepare list of {@link JobDto}s that matches with
   * the provided {@link Job}. See com.linkedin.openhouse.housetables.dto.model.JobTable#match for
   * the definition of match.
   *
   * @param job object served as filtering condition.
   * @return list of {@link JobDto}s that matches the provided {@link Job}
   */
  List<JobDto> getAllJobs(Job job);

  /** Given a jobId, delete the entry from the Table. */
  void deleteJob(String jobId);

  /**
   * Create or update a {@link Job} row in House table.
   *
   * @param job The object attempted to be used for update/creation.
   * @return A pair of object: The first {@link JobDto} is the actual saved object. The second
   *     boolean is set to true if overwritten occurred. This is to differentiate between creation
   *     and update of {@link JobDto}
   */
  Pair<JobDto, Boolean> putJob(Job job);
}
