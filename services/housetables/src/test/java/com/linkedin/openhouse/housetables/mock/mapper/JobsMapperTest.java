package com.linkedin.openhouse.housetables.mock.mapper;

import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRowPrimaryKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class JobsMapperTest {

  private static final Job TEST_JOB =
      Job.builder().jobId("id1").state("QUEUED").version("1").build();
  private static final JobDto TEST_JOB_DTO =
      JobDto.builder().jobId("id1").state("QUEUED").version("1").build();
  private static final JobRow TEST_JOB_ROW =
      JobRow.builder().jobId("id1").state("QUEUED").version(1L).build();
  private static final JobRowPrimaryKey TEST_JOB_ROW_PK =
      JobRowPrimaryKey.builder().jobId("id1").build();
  private static final JobIcebergRow TEST_JOB_ICEBERG_ROW =
      JobIcebergRow.builder().jobId("id1").state("QUEUED").version("1").build();
  private static final JobIcebergRowPrimaryKey TEST_JOB_ICEBERG_ROW_PK =
      JobIcebergRowPrimaryKey.builder().jobId("id1").build();

  @Autowired JobMapper jobMapper;

  @Test
  void toJobRow() {
    Assertions.assertEquals(TEST_JOB_ROW, jobMapper.toJobRow(TEST_JOB));

    Assertions.assertEquals(TEST_JOB_ROW, jobMapper.toJobRow(TEST_JOB_ICEBERG_ROW));
  }

  @Test
  void toJobDto() {
    Assertions.assertEquals(TEST_JOB_DTO, jobMapper.toJobDto(TEST_JOB_ROW));

    Assertions.assertEquals(TEST_JOB_DTO, jobMapper.toJobDto(TEST_JOB));
  }

  @Test
  void toJob() {
    Assertions.assertEquals(TEST_JOB, jobMapper.toJob(TEST_JOB_DTO));
  }

  @Test
  void toJobIcebergRow() {
    Assertions.assertEquals(
        TEST_JOB_ICEBERG_ROW.getJobId(), jobMapper.toJobIcebergRow(TEST_JOB_ROW).getJobId());

    Assertions.assertEquals(
        TEST_JOB_ICEBERG_ROW.getState(), jobMapper.toJobIcebergRow(TEST_JOB_ROW).getState());

    Assertions.assertEquals(
        TEST_JOB_ICEBERG_ROW.getVersion(), jobMapper.toJobIcebergRow(TEST_JOB_ROW).getVersion());
  }

  @Test
  void toJobIcebergRowPrimaryKey() {
    Assertions.assertEquals(
        TEST_JOB_ICEBERG_ROW_PK.getJobId(),
        jobMapper.toJobIcebergRowPrimaryKey(TEST_JOB_ROW_PK).getJobId());
  }

  @Test
  void toJobRowPrimaryKey() {
    Assertions.assertEquals(TEST_JOB_ROW_PK, jobMapper.toJobRowPrimaryKey(TEST_JOB));

    Assertions.assertEquals(TEST_JOB_ROW_PK, jobMapper.toJobRowPrimaryKey(TEST_JOB_ROW));
  }
}
