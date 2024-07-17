package com.linkedin.openhouse.tables.mock.controller;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;

/** Enable authorization for some Controller unit-tests. */
public class AuthorizationPropertiesInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        applicationContext, "cluster.security.tables.authorization.enabled=true");
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        applicationContext,
        "cluster.security.token.interceptor.classname=com.linkedin.openhouse.common.security.DummyTokenInterceptor");
  }
}
