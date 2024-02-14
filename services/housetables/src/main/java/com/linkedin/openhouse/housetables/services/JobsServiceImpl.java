package com.linkedin.openhouse.housetables.services;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

@Component
public class JobsServiceImpl implements JobsService {

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @Autowired JobMapper jobMapper;

  @Override
  public JobDto getJob(String jobId) {
    JobRow jobRow;

    try {
      jobRow =
          htsRepository
              .findById(JobRowPrimaryKey.builder().jobId(jobId).build())
              .orElseThrow(NoSuchElementException::new);
    } catch (NoSuchElementException ne) {
      throw new NoSuchEntityException("job", jobId, ne);
    }

    return jobMapper.toJobDto(jobRow);
  }

  @Override
  public List<JobDto> getAllJobs(Job job) {
    JobDto targetJobDto = jobMapper.toJobDto(job);

    return StreamSupport.stream(htsRepository.findAll().spliterator(), false)
        .map(jobRow -> jobMapper.toJobDto(jobRow))
        .filter(x -> x.match(targetJobDto))
        .collect(Collectors.toList());
  }

  @Override
  public void deleteJob(String jobId) {
    JobRowPrimaryKey key = JobRowPrimaryKey.builder().jobId(jobId).build();
    if (!htsRepository.existsById(key)) {
      throw new NoSuchEntityException("job", jobId);
    }
    htsRepository.deleteById(key);
  }

  @Override
  public Pair<JobDto, Boolean> putJob(Job job) {
    JobRowPrimaryKey key = jobMapper.toJobRowPrimaryKey(job);
    boolean existed = htsRepository.existsById(key);

    JobRow targetJobRow = jobMapper.toJobRow(job);
    JobDto returnedDto;

    if (!existed) {
      // while creating the row, ignore user provided version and set 1L
      targetJobRow = targetJobRow.toBuilder().version(1L).build();
    }
    try {
      returnedDto = jobMapper.toJobDto(htsRepository.save(targetJobRow));
    } catch (CommitFailedException | ObjectOptimisticLockingFailureException ce) {
      throw new EntityConcurrentModificationException(
          String.format(
              "jobId : %s, version: %s",
              job.getJobId(),
              job.getVersion(),
              "The requested user table has been modified/created by other processes."),
          key.toString(),
          ce);
    }

    return Pair.of(returnedDto, existed);
  }
}
