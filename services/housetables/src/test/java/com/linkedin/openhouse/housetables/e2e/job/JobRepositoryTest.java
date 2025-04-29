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

  private final JobRow testJobRow =
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
          .engineType("LIVY")
          .build();

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @Autowired JobMapper jobMapper;

  @BeforeEach
  public void setup() {
    htsRepository.deleteAll();
  }

  @Test
  public void testSave() {
    htsRepository.save(testJobRow);
    JobRow actual =
        htsRepository
            .findById(jobMapper.toJobRowPrimaryKey(testJobRow))
            .orElse(JobRow.builder().build());

    Assertions.assertTrue(isJobRowEqual(testJobRow, actual));
    htsRepository.delete(actual);
  }

  @Test
  public void testDelete() {
    htsRepository.save(testJobRow);
    JobRowPrimaryKey key = jobMapper.toJobRowPrimaryKey(testJobRow);
    assertThat(htsRepository.existsById(key)).isTrue();
    htsRepository.deleteById(key);
    assertThat(htsRepository.existsById(key)).isFalse();
  }

  @Test
  public void testSaveJobWithConflict() {
    Long currentVersion = htsRepository.save(testJobRow).getVersion();

    // test update at wrong version
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> htsRepository.save(testJobRow.toBuilder().version(100L).build()));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    // test update at correct version
    Assertions.assertNotEquals(
        htsRepository
            .save(testJobRow.toBuilder().version(currentVersion).startTimeMs(1L).build())
            .getVersion(),
        currentVersion);

    // test update at older version
    exception = Assertions.assertThrows(Exception.class, () -> htsRepository.save(testJobRow));
    Assertions.assertTrue(
        exception instanceof ObjectOptimisticLockingFailureException
            | exception instanceof EntityConcurrentModificationException);

    htsRepository.deleteById(JobRowPrimaryKey.builder().jobId(testJobRow.getJobId()).build());
  }

  private Boolean isJobRowEqual(JobRow expected, JobRow actual) {
    return expected.toBuilder().version(1L).build().equals(actual.toBuilder().version(1L).build());
  }
}
