package com.linkedin.openhouse.tables.mock.controller;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/** Enable authorization for some Controller unit-tests. */
public class AuthorizationPropertiesInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    System.setProperty("cluster.security.tables.authorization.enabled", "true");
  }
}
