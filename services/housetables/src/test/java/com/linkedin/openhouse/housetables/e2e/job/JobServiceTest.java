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

  private final JobRow TEST_JOB_ROW =
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
  private final JobRow TEST_JOB_ROW_2 = TEST_JOB_ROW.toBuilder().jobId("id2").state("X").build();
  private final JobRow TEST_JOB_ROW_3 = TEST_JOB_ROW.toBuilder().jobId("id3").state("X").build();

  @Autowired JobsService jobsService;

  @Autowired JobMapper jobMapper;

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @BeforeEach
  public void setup() {
    htsRepository.save(TEST_JOB_ROW);
    htsRepository.save(TEST_JOB_ROW_2);
    htsRepository.save(TEST_JOB_ROW_3);
  }

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
  }

  @Test
  public void testJobGet() {
    Assertions.assertEquals(
        withoutVersion(jobMapper.toJobDto(TEST_JOB_ROW)),
        withoutVersion(jobsService.getJob(TEST_JOB_ROW.getJobId())));
    Assertions.assertEquals(
        withoutVersion(jobMapper.toJobDto(TEST_JOB_ROW_2)),
        withoutVersion(jobsService.getJob(TEST_JOB_ROW_2.getJobId())));
  }

  @Test
  public void testJobQuery() {
    List<JobDto> expected;
    List<JobDto> actual;

    // test filter on state
    expected =
        Arrays.asList(jobMapper.toJobDto(TEST_JOB_ROW_2), jobMapper.toJobDto(TEST_JOB_ROW_3));
    actual = jobsService.getAllJobs(Job.builder().state(TEST_JOB_ROW_2.getState()).build());
    Assertions.assertTrue(
        CollectionUtils.isEqualCollection(withoutVersion(expected), withoutVersion(actual)));

    // test filter on id
    expected = Collections.singletonList(jobMapper.toJobDto(TEST_JOB_ROW));
    actual = jobsService.getAllJobs(Job.builder().jobId(TEST_JOB_ROW.getJobId()).build());
    Assertions.assertTrue(
        CollectionUtils.isEqualCollection(withoutVersion(expected), withoutVersion(actual)));
  }

  @Test
  public void testJobDelete() {
    Assertions.assertDoesNotThrow(() -> jobsService.deleteJob(TEST_JOB_ROW.getJobId()));
    NoSuchEntityException noSuchEntityException =
        Assertions.assertThrows(
            NoSuchEntityException.class, () -> jobsService.deleteJob(TEST_JOB_ROW.getJobId()));
    Assertions.assertTrue(noSuchEntityException.getMessage().contains(TEST_JOB_ROW.getJobId()));
  }

  @Test
  public void testJobUpdate() {
    String atVersion = jobsService.getJob(TEST_JOB_ROW.getJobId()).getVersion();

    Job updatedJob =
        Job.builder()
            .jobId(TEST_JOB_ROW.getJobId())
            .state("Y")
            .version(atVersion)
            .jobName("jobName")
            .clusterId("testCluster")
            .executionId("1")
            .build();
    Pair<JobDto, Boolean> result = jobsService.putJob(updatedJob);

    Assertions.assertTrue(result.getSecond()); // update occurred not creation, returns true
    Assertions.assertNotEquals(result.getFirst().getVersion(), updatedJob.getVersion());

    jobsService.deleteJob(TEST_JOB_ROW_2.getJobId());
    Pair<JobDto, Boolean> result2 =
        jobsService.putJob(jobMapper.toJob(jobMapper.toJobDto(TEST_JOB_ROW_2)));
    Assertions.assertFalse(result2.getSecond());
  }

  private List<JobDto> withoutVersion(List<JobDto> actual) {
    return actual.stream().map(this::withoutVersion).collect(Collectors.toList());
  }

  private JobDto withoutVersion(JobDto actual) {
    return actual.toBuilder().version("").build();
  }
}
