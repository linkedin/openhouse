package com.linkedin.openhouse.tablestest;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
@ComponentScan(
    basePackages = {
      "com.linkedin.openhouse.tables.api",
      "com.linkedin.openhouse.tables.audit",
      "com.linkedin.openhouse.tables.authorization",
      "com.linkedin.openhouse.tables.dto.mapper",
      "com.linkedin.openhouse.tables.utils",
      "com.linkedin.openhouse.tables.controller",
      "com.linkedin.openhouse.tables.services",
      "com.linkedin.openhouse.tables.config",
      "com.linkedin.openhouse.tables.toggle.repository",
      "com.linkedin.openhouse.tables.toggle",
      "com.linkedin.openhouse.internal.catalog.toggle",
      "com.linkedin.openhouse.internal.catalog",
      "com.linkedin.openhouse.cluster.configs",
      "com.linkedin.openhouse.cluster.storage",
      "com.linkedin.openhouse.tables.repository",
      "com.linkedin.openhouse.common.exception.handler",
      "com.linkedin.openhouse.common.audit"
    })
@EntityScan(
    basePackages = {
      "com.linkedin.openhouse.tables.model",
      "com.linkedin.openhouse.tables.toggle.model",
      "com.linkedin.openhouse.internal.catalog.model"
    })
@EnableAutoConfiguration(
    exclude = {SecurityAutoConfiguration.class, ManagementWebSecurityAutoConfiguration.class})
public class SpringH2TestApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringH2TestApplication.class, args);
  }

  /**
   * File secure used for testing purpose. We cannot directly use the actual
   * SnapshotInspector#fileSecurer as that changes file to a user group that is not guaranteed to
   * exist across different platforms thus creating environment dependencies for unit tests.
   */
  @Bean
  @Primary
  Consumer<Supplier<Path>> provideTestFileSecurer() {
    return pathSupplier -> {
      // This is a no-op Consumer. It does nothing with the supplied Path.
    };
  }
}
