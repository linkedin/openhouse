package com.linkedin.openhouse.jobs.mock;

import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.exception.JobEngineException;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsEngineProperties;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import com.linkedin.openhouse.jobs.services.HouseJobHandle;
import com.linkedin.openhouse.jobs.services.JobsCoordinatorManager;
import com.linkedin.openhouse.jobs.services.livy.LivyJobHandle;
import com.linkedin.openhouse.jobs.services.livy.LivyJobsCoordinator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class JobsCoordinatorManagerTest {
  @MockBean private LivyJobsCoordinator livyJobsCoordinator;
  @MockBean private LivyJobHandle livyJobHandle;

  @Test
  public void testJobsCoordinatorManagerInit() {
    JobsEngineProperties jobsEngineProperties = new JobsEngineProperties();
    jobsEngineProperties.setEngineType("LIVY");
    jobsEngineProperties.setEngineUri("http://localhost:8998");
    jobsEngineProperties.setCoordinatorClassName(
        "com.linkedin.openhouse.jobs.services.livy.LivyJobsCoordinator");
    JobsProperties jobsProperties = new JobsProperties();
    jobsProperties.setDefaultEngine("LIVY");
    jobsProperties.setEngines(ImmutableList.of(jobsEngineProperties));
    JobsCoordinatorManager jobsCoordinatorManager = JobsCoordinatorManager.from(jobsProperties);

    Assertions.assertEquals(1, jobsCoordinatorManager.getCoordinators().size());
    Assertions.assertTrue(jobsCoordinatorManager.getCoordinators().containsKey("LIVY"));
    Assertions.assertEquals("LIVY", jobsCoordinatorManager.getDefaultEngineType());
  }

  @Test
  public void testSubmit() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    JobLaunchConf conf = JobLaunchConf.builder().engineType("LIVY").build();
    when(livyJobsCoordinator.submit(conf)).thenReturn(livyJobHandle);
    HouseJobHandle actualHandle = jobsCoordinatorManager.submit(conf);

    Assertions.assertEquals(actualHandle, livyJobHandle);
  }

  @Test
  public void testSubmitEngineNotFound() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    JobLaunchConf conf = JobLaunchConf.builder().engineType("SPARK").build();

    Assertions.assertThrows(
        JobEngineException.class,
        () -> jobsCoordinatorManager.submit(conf),
        "No coordinator found for engine type: SPARK");
  }

  @Test
  public void testSubmitEngineTypeNull() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    JobLaunchConf conf = JobLaunchConf.builder().build();
    when(livyJobsCoordinator.submit(conf)).thenReturn(livyJobHandle);
    HouseJobHandle actualHandle = jobsCoordinatorManager.submit(conf);

    Assertions.assertEquals(actualHandle, livyJobHandle);
  }

  @Test
  public void testObtainHandle() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    when(livyJobsCoordinator.obtainHandle("test-id")).thenReturn(livyJobHandle);
    HouseJobHandle actualHandle = jobsCoordinatorManager.obtainHandle("LIVY", "test-id");

    Assertions.assertEquals(actualHandle, livyJobHandle);
  }

  @Test
  public void testObtainHandleEngineNotFound() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    when(livyJobsCoordinator.obtainHandle("test-id")).thenReturn(livyJobHandle);

    Assertions.assertThrows(
        JobEngineException.class,
        () -> jobsCoordinatorManager.obtainHandle("SPARK", "test-id"),
        "No coordinator found for engine type: SPARK");
  }

  @Test
  public void testObtainHandleEngineTypeNull() {
    JobsCoordinatorManager jobsCoordinatorManager =
        new JobsCoordinatorManager(ImmutableMap.of("LIVY", livyJobsCoordinator), "LIVY");
    when(livyJobsCoordinator.obtainHandle("test-id")).thenReturn(livyJobHandle);
    HouseJobHandle actualHandle = jobsCoordinatorManager.obtainHandle(null, "test-id");

    Assertions.assertEquals(actualHandle, livyJobHandle);
  }
}
