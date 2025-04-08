package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import com.linkedin.openhouse.jobs.services.JobsCoordinatorManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = {JobsProperties.class, JobsCoordinatorManager.class})
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class JobsCoordinatorManagerTest {
  @Autowired private JobsProperties properties;
  @Autowired private JobsCoordinatorManager jobsCoordinatorManager;

  @Bean
  JobsCoordinatorManager createJobsCoordinatorManager() {
    return JobsCoordinatorManager.from(properties);
  }

  @Test
  public void testSubmit() {
    JobLaunchConf conf = JobLaunchConf.builder().engineType("LIVY").build();
    jobsCoordinatorManager.submit(conf);

    JobLaunchConf conf2 = JobLaunchConf.builder().engineType("SPARK").build();
    jobsCoordinatorManager.submit(conf2);
  }

  @Test
  public void testObtainHandle() {
    jobsCoordinatorManager.obtainHandle("LIVY", "test-id");

    jobsCoordinatorManager.obtainHandle(null, "test-id");

    jobsCoordinatorManager.obtainHandle("SPARK", "test-id");
  }
}
