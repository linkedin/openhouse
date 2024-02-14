package com.linkedin.openhouse.jobs.mock;

import com.google.common.collect.Maps;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.config.JobsProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = {JobsProperties.class})
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class JobsPropertiesTest {
  @Autowired private JobsProperties properties;

  @Test
  void test() {
    Assertions.assertEquals("test-uri", properties.getEngineUri());
    Assertions.assertEquals("test-uri", properties.getStorageUri());
    Assertions.assertEquals("test-class-name", properties.getCoordinatorClassName());
    final Map<String, String> expectedDefaultSparkProperties = new HashMap<>();
    expectedDefaultSparkProperties.put("dp1", "dv1");
    expectedDefaultSparkProperties.put("dp2", "dv2");

    final Map<String, String> expectedSparkProperties = new HashMap<>();
    expectedSparkProperties.put("p1", "v1");
    expectedSparkProperties.put("p2", "v2");
    Map<String, String> executionTags = Maps.newHashMap();
    executionTags.put("pool", "dev");

    final List<String> expectedOFDArgs = new ArrayList<>();
    expectedOFDArgs.add("--trashDir");
    expectedOFDArgs.add(".trash");
    List<JobLaunchConf> expected =
        Arrays.asList(
            JobLaunchConf.builder()
                .type("test-job-1")
                .className("job-class-name")
                .args(Arrays.asList("arg1", "arg2"))
                .jarPath("job-jar-path")
                .executionTags(Maps.newHashMap())
                .dependencies(Collections.singletonList("d1"))
                .sparkProperties(expectedSparkProperties)
                .build(),
            JobLaunchConf.builder()
                .type("test-job-2")
                .className("job-class-name-2")
                .args(Collections.emptyList())
                .jarPath("default-jar-path")
                .executionTags(executionTags)
                .dependencies(Collections.emptyList())
                .sparkProperties(expectedDefaultSparkProperties)
                .build(),
            JobLaunchConf.builder()
                .type("test-job-3")
                .className("job-3-class-name")
                .args(Collections.emptyList())
                .jarPath("default-jar-path")
                .executionTags(executionTags)
                .dependencies(Collections.singletonList("d3"))
                .sparkProperties(expectedDefaultSparkProperties)
                .build(),
            JobLaunchConf.builder()
                .type("RETENTION")
                .className("job-3-class-name")
                .args(Collections.emptyList())
                .jarPath("default-jar-path")
                .executionTags(executionTags)
                .dependencies(Collections.singletonList("d3"))
                .sparkProperties(expectedDefaultSparkProperties)
                .build(),
            JobLaunchConf.builder()
                .type("ORPHAN_FILES_DELETION")
                .className("job-3-class-name")
                .args(Collections.emptyList())
                .jarPath("default-jar-path")
                .executionTags(executionTags)
                .dependencies(Collections.singletonList("d3"))
                .args(expectedOFDArgs)
                .sparkProperties(expectedDefaultSparkProperties)
                .build());
    Assertions.assertEquals(expected, properties.getApps());
    Assertions.assertEquals(
        expectedDefaultSparkProperties, properties.getApps().get(2).getSparkProperties());
    Assertions.assertEquals(
        expectedSparkProperties, properties.getApps().get(0).getSparkProperties());
    Assertions.assertEquals(null, properties.getAuthTokenPath());
  }
}
