package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.model.JobConf;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.model.JobDtoPrimaryKey;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepository;
import com.linkedin.openhouse.jobs.services.HouseJobHandle;
import com.linkedin.openhouse.jobs.services.HouseJobsCoordinator;
import com.linkedin.openhouse.jobs.services.JobInfo;
import com.linkedin.openhouse.jobs.services.JobsRegistry;
import com.linkedin.openhouse.jobs.services.JobsService;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class JobsServiceTest {
  @Autowired private JobsService service;

  @MockBean(name = "repository")
  private JobsInternalRepository repository;

  @MockBean private JobsRegistry jobsRegistry;
  @MockBean private HouseJobsCoordinator jobsCoordinator;
  @MockBean private HouseJobHandle jobHandle;
  @MockBean private JobInfo jobInfo;

  @Test
  void testCreate() {
    JobDto job = JobModelConstants.JOB_DTO;
    CreateJobRequestBody requestBody =
        CreateJobRequestBody.builder()
            .jobName(job.getJobName())
            .clusterId(job.getClusterId())
            .jobConf(job.getJobConf())
            .build();
    Mockito.when(repository.save(Mockito.any())).thenReturn(job);
    Mockito.when(jobsRegistry.createLaunchConf(Mockito.any(), Mockito.any()))
        .thenReturn(JobLaunchConf.builder().build());
    Mockito.when(jobsCoordinator.submit(Mockito.any())).thenReturn(jobHandle);
    Mockito.when(jobHandle.getInfo()).thenReturn(jobInfo);
    Mockito.when(jobInfo.getExecutionId()).thenReturn(job.getExecutionId());
    Assertions.assertEquals(job, service.create(requestBody));
  }

  @Test
  void testCreateWithSparkMemory() {
    Map<String, String> executionConf = new HashMap<>();
    executionConf.put("memory", "4G");
    JobDto job =
        JobModelConstants.JOB_DTO.toBuilder()
            .jobConf(
                JobConf.builder()
                    .executionConf(executionConf)
                    .jobType(JobConf.JobType.RETENTION)
                    .build())
            .build();
    CreateJobRequestBody requestBody =
        CreateJobRequestBody.builder()
            .jobName(job.getJobName())
            .clusterId(job.getClusterId())
            .jobConf(job.getJobConf())
            .build();
    Mockito.when(repository.save(Mockito.any())).thenReturn(job);
    Mockito.when(jobsRegistry.createLaunchConf(Mockito.any(), Mockito.any()))
        .thenReturn(JobLaunchConf.builder().build());
    Mockito.when(jobsCoordinator.submit(Mockito.any())).thenReturn(jobHandle);
    Mockito.when(jobHandle.getInfo()).thenReturn(jobInfo);
    Mockito.when(jobInfo.getExecutionId()).thenReturn(job.getExecutionId());
    JobDto actualJobDto = service.create(requestBody);
    Assertions.assertEquals("4G", actualJobDto.getJobConf().getExecutionConf().get("memory"));
    Assertions.assertEquals(job, actualJobDto);
  }

  @Test
  void testGet() {
    JobDto job = JobModelConstants.JOB_DTO;
    JobDtoPrimaryKey key = JobDtoPrimaryKey.builder().jobId(job.getJobId()).build();
    Mockito.when(repository.findById(key)).thenReturn(Optional.of(job));
    Assertions.assertEquals(job, service.get(job.getJobId()));
  }

  @Test
  void testCancel() {
    JobDto job = JobModelConstants.JOB_DTO;
    JobDtoPrimaryKey key = JobDtoPrimaryKey.builder().jobId(job.getJobId()).build();
    Mockito.when(repository.findById(key)).thenReturn(Optional.of(job));
    Mockito.when(repository.save(Mockito.any())).thenReturn(job);
    Assertions.assertEquals(job, service.get(job.getJobId()));
  }
}
