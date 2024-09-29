package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import com.linkedin.openhouse.cluster.storage.filesystem.HdfsStorageProvider;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import com.linkedin.openhouse.jobs.model.JobConf;
import com.linkedin.openhouse.jobs.services.JobsRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(
    classes = {JobsProperties.class, HdfsStorageProvider.class, ClusterProperties.class})
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class JobsRegistryTest {
  @Autowired private JobsProperties properties;
  @MockBean private JobConf jobConf;
  @Autowired private FsStorageProvider fsStorageProvider;

  @Test
  void testShouldCreateLaunchConf() {
    Map<String, String> propertyMap = fsStorageProvider.storageProperties();
    propertyMap.put("fs.defaultFS", "default");
    JobsRegistry jobsRegistry = JobsRegistry.from(properties, propertyMap);
    Mockito.when(jobConf.getJobType()).thenReturn(JobConf.JobType.RETENTION);
    Mockito.when(jobConf.getArgs()).thenReturn(new ArrayList<>());
    Map<String, String> executionConf = new HashMap<>();
    executionConf.put("spark.driver.memory", "5G");
    executionConf.put("spark.driver.maxResultSize", "0");
    Mockito.when(jobConf.getExecutionConf()).thenReturn(executionConf);
    JobLaunchConf launchConf = jobsRegistry.createLaunchConf("jobId", jobConf);
    Assertions.assertEquals(launchConf.getJarPath(), "default-jar-path");
    Assertions.assertTrue(launchConf.getExecutionTags().keySet().contains("pool"));
    Assertions.assertFalse(launchConf.getArgs().contains("--trashDir"));
    Assertions.assertFalse(
        launchConf
            .getSparkProperties()
            .keySet()
            .contains("spark.sql.catalog.openhouse.auth-token"));
    Assertions.assertTrue(launchConf.getSparkProperties().keySet().contains("spark.driver.memory"));
    Assertions.assertTrue(
        launchConf.getSparkProperties().keySet().contains("spark.driver.maxResultSize"));
    Assertions.assertTrue(launchConf.getSparkProperties().containsKey("fs.defaultFS"));
    Assertions.assertEquals(launchConf.getArgs().size(), 4);
  }

  @Test
  void testDefaultLaunchConfUnmodified() {
    Map<String, String> propertyMap = fsStorageProvider.storageProperties();
    propertyMap.put("fs.defaultFS", "default");
    JobsRegistry jobsRegistry = JobsRegistry.from(properties, propertyMap);
    Mockito.when(jobConf.getJobType()).thenReturn(JobConf.JobType.RETENTION);
    Mockito.when(jobConf.getArgs()).thenReturn(new ArrayList<>());
    Map<String, String> executionConf = new HashMap<>();
    executionConf.put("spark.driver.memory", "5G");
    executionConf.put("spark.driver.maxResultSize", "0");
    Mockito.when(jobConf.getExecutionConf()).thenReturn(executionConf);
    JobLaunchConf launchConf = jobsRegistry.createLaunchConf("jobId", jobConf);
    Assertions.assertTrue(launchConf.getSparkProperties().keySet().contains("spark.driver.memory"));
    Assertions.assertTrue(
        launchConf.getSparkProperties().keySet().contains("spark.driver.maxResultSize"));
    Map<String, String> emptyExecutionConf = new HashMap<>();
    Mockito.when(jobConf.getExecutionConf()).thenReturn(emptyExecutionConf);
    launchConf = jobsRegistry.createLaunchConf("jobId", jobConf);
    Assertions.assertFalse(
        launchConf.getSparkProperties().keySet().contains("spark.driver.memory"));
    Assertions.assertFalse(
        launchConf.getSparkProperties().keySet().contains("spark.driver.maxResultSize"));
  }

  @Test
  void testLaunchConfSholdHaveTrashDirForOFD() {
    Map<String, String> propertyMap = fsStorageProvider.storageProperties();
    propertyMap.put("fs.defaultFS", "default");
    JobsRegistry jr = JobsRegistry.from(properties, propertyMap);
    Mockito.when(jobConf.getJobType()).thenReturn(JobConf.JobType.ORPHAN_FILES_DELETION);
    Mockito.when(jobConf.getArgs()).thenReturn(new ArrayList<>());
    JobLaunchConf launchConf = jr.createLaunchConf("jobId", jobConf);
    Assertions.assertTrue(launchConf.getArgs().contains("--trashDir"));
    Assertions.assertEquals(launchConf.getArgs().size(), 6);
  }
}
