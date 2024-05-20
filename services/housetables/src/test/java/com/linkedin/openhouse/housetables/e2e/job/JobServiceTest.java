package com.linkedin.openhouse.housetables.e2e.job;

import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.services.JobsService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.util.Pair;

@SpringBootTest(classes = SpringH2HtsApplication.class)
public class JobServiceTest {

  private final JobRow testJobRow =
      JobRow.builder()
          .jobId("id1")
          .state("Q")
          .version(1L)
          .jobName("jobName")
          .jobConf("jobConf")
          .clusterId("testCluster")
          .creationTimeMs(1651016746000L)
          .startTimeMs(1651016750000L)
          .finishTimeMs(1651017746000L)
          .lastUpdateTimeMs(1651017746000L)
          .heartbeatTimeMs(1651017746000L)
          .executionId("1")
          .build();
  private final JobRow testJobRow2 = testJobRow.toBuilder().jobId("id2").state("X").build();
  private final JobRow testJobRow3 = testJobRow.toBuilder().jobId("id3").state("X").build();

  @Autowired JobsService jobsService;

  @Autowired JobMapper jobMapper;

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @BeforeEach
  public void setup() {
    htsRepository.save(testJobRow);
    htsRepository.save(testJobRow2);
    htsRepository.save(testJobRow3);
  }

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
  }

  @Test
  public void testJobGet() {
    Assertions.assertEquals(
        withoutVersion(jobMapper.toJobDto(testJobRow)),
        withoutVersion(jobsService.getJob(testJobRow.getJobId())));
    Assertions.assertEquals(
        withoutVersion(jobMapper.toJobDto(testJobRow2)),
        withoutVersion(jobsService.getJob(testJobRow2.getJobId())));
  }

  @Test
  public void testJobQuery() {
    List<JobDto> expected;
    List<JobDto> actual;

    // test filter on state
    expected = Arrays.asList(jobMapper.toJobDto(testJobRow2), jobMapper.toJobDto(testJobRow3));
    actual = jobsService.getAllJobs(Job.builder().state(testJobRow2.getState()).build());
    Assertions.assertTrue(
        CollectionUtils.isEqualCollection(withoutVersion(expected), withoutVersion(actual)));

    // test filter on id
    expected = Collections.singletonList(jobMapper.toJobDto(testJobRow));
    actual = jobsService.getAllJobs(Job.builder().jobId(testJobRow.getJobId()).build());
    Assertions.assertTrue(
        CollectionUtils.isEqualCollection(withoutVersion(expected), withoutVersion(actual)));
  }

  @Test
  public void testJobDelete() {
    Assertions.assertDoesNotThrow(() -> jobsService.deleteJob(testJobRow.getJobId()));
    NoSuchEntityException noSuchEntityException =
        Assertions.assertThrows(
            NoSuchEntityException.class, () -> jobsService.deleteJob(testJobRow.getJobId()));
    Assertions.assertTrue(noSuchEntityException.getMessage().contains(testJobRow.getJobId()));
  }

  @Test
  public void testJobUpdate() {
    String atVersion = jobsService.getJob(testJobRow.getJobId()).getVersion();

    Job updatedJob =
        Job.builder()
            .jobId(testJobRow.getJobId())
            .state("Y")
            .version(atVersion)
            .jobName("jobName")
            .clusterId("testCluster")
            .executionId("1")
            .build();
    Pair<JobDto, Boolean> result = jobsService.putJob(updatedJob);

    Assertions.assertTrue(result.getSecond()); // update occurred not creation, returns true
    Assertions.assertNotEquals(result.getFirst().getVersion(), updatedJob.getVersion());

    jobsService.deleteJob(testJobRow2.getJobId());
    Pair<JobDto, Boolean> result2 =
        jobsService.putJob(jobMapper.toJob(jobMapper.toJobDto(testJobRow2)));
    Assertions.assertFalse(result2.getSecond());
  }

  private List<JobDto> withoutVersion(List<JobDto> actual) {
    return actual.stream().map(this::withoutVersion).collect(Collectors.toList());
  }

  private JobDto withoutVersion(JobDto actual) {
    return actual.toBuilder().version("").build();
  }
}
