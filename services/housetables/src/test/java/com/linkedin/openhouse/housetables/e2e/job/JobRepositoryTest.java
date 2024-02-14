package com.linkedin.openhouse.housetables.e2e.job;

import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class JobRepositoryTest {

  private final JobRow TEST_JOB_ROW =
      JobRow.builder()
          .jobId("id1")
          .version(1L)
          .state("QUEUED")
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

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @Autowired JobMapper jobMapper;

  @BeforeEach
  public void setup() {
    htsRepository.deleteAll();
  }

  @Test
  public void testSave() {
    htsRepository.save(TEST_JOB_ROW);
    JobRow actual =
        htsRepository
            .findById(jobMapper.toJobRowPrimaryKey(TEST_JOB_ROW))
            .orElse(JobRow.builder().build());

    Assertions.assertTrue(isJobRowEqual(TEST_JOB_ROW, actual));
    htsRepository.delete(actual);
  }

  @Test
  public void testDelete() {
    htsRepository.save(TEST_JOB_ROW);
    JobRowPrimaryKey key = jobMapper.toJobRowPrimaryKey(TEST_JOB_ROW);
    assertThat(htsRepository.existsById(key)).isTrue();
    htsRepository.deleteById(key);
    assertThat(htsRepository.existsById(key)).isFalse();
  }

  @Test
  public void testSaveJobWithConflict() {
    Long currentVersion = htsRepository.save(TEST_JOB_ROW).getVersion();

    // test update at wrong version
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> htsRepository.save(TEST_JOB_ROW.toBuilder().version(100L).build()));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    // test update at correct version
    Assertions.assertNotEquals(
        htsRepository
            .save(TEST_JOB_ROW.toBuilder().version(currentVersion).startTimeMs(1L).build())
            .getVersion(),
        currentVersion);

    // test update at older version
    exception = Assertions.assertThrows(Exception.class, () -> htsRepository.save(TEST_JOB_ROW));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    htsRepository.deleteById(JobRowPrimaryKey.builder().jobId(TEST_JOB_ROW.getJobId()).build());
  }

  private Boolean isJobRowEqual(JobRow expected, JobRow actual) {
    return expected.toBuilder().version(1L).build().equals(actual.toBuilder().version(1L).build());
  }
}
