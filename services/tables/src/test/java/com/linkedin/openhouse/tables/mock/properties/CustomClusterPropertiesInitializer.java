package com.linkedin.openhouse.tables.mock.properties;

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
      file =
          ResourceUtils.getFile(
              CustomClusterPropertiesTest.class.getResource("/cluster-test-properties.yaml"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    System.setProperty("OPENHOUSE_CLUSTER_CONFIG_PATH", file.getAbsolutePath());
  }
}
