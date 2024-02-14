package com.linkedin.openhouse.tables.mock.properties;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public class DefaultClusterPropertiesInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.setProperty("OPENHOUSE_CLUSTER_CONFIG_PATH", "");
  }
}
