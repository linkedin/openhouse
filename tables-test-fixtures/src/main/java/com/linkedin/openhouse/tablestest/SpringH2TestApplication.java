package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.cluster.storage.filesystem.FsStorageProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.beans.factory.annotation.Autowired;
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
      "com.linkedin.openhouse.tables.toggle",
      "com.linkedin.openhouse.tables.authorization",
      "com.linkedin.openhouse.tables.dto.mapper",
      "com.linkedin.openhouse.tables.utils",
      "com.linkedin.openhouse.tables.controller",
      "com.linkedin.openhouse.tables.services",
      "com.linkedin.openhouse.tables.config",
      "com.linkedin.openhouse.tables.toggle.repository",
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
      "com.linkedin.openhouse.internal.catalog.model"
    })
@EnableAutoConfiguration(
    exclude = {SecurityAutoConfiguration.class, ManagementWebSecurityAutoConfiguration.class})
public class SpringH2TestApplication {
  @Autowired protected FsStorageProvider fsStorageProvider;

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
      try {
        fsStorageProvider
            .storageClient()
            .setPermission(
                pathSupplier.get(), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    };
  }
}
