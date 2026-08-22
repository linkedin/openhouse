package com.linkedin.openhouse.tables.mock.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.linkedin.openhouse.tables.api.handler.IcebergRestApiHandler;
import com.linkedin.openhouse.tables.controller.IcebergRestCatalogController;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

public class IcebergRestFeatureFlagTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

  @Test
  void controllerIsDisabledByDefault() {
    contextRunner.run(
        context -> assertThat(context).doesNotHaveBean(IcebergRestCatalogController.class));
  }

  @Test
  void controllerCanBeEnabled() {
    contextRunner
        .withPropertyValues("cluster.tables.iceberg-rest.enabled=true")
        .run(context -> assertThat(context).hasSingleBean(IcebergRestCatalogController.class));
  }

  @Configuration(proxyBeanMethods = false)
  @Import(IcebergRestCatalogController.class)
  static class TestConfiguration {

    @Bean
    IcebergRestApiHandler icebergRestApiHandler() {
      return mock(IcebergRestApiHandler.class);
    }
  }
}
