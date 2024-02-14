package com.linkedin.openhouse.jobs.mock;

import java.io.File;
import java.io.FileNotFoundException;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.ResourceUtils;

public class CustomClusterPropertiesInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    File file = null;
    try {
      file = ResourceUtils.getFile(this.getClass().getResource("/test-local-jobs.yaml"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    System.setProperty("OPENHOUSE_JOBS_CONFIG_PATH", file.getAbsolutePath());
    file = null;
    try {
      file = ResourceUtils.getFile(this.getClass().getResource("/test-local-cluster.yaml"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    System.setProperty("OPENHOUSE_CLUSTER_CONFIG_PATH", file.getAbsolutePath());
  }
}
