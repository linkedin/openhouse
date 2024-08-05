package com.linkedin.openhouse.common.test.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.support.TestPropertySourceUtils;

public class PropertyOverrideContextInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    Path tempDirectory = null;
    List<String> testAllowedClientNameValues = Arrays.asList("trino", "spark");
    try {
      tempDirectory = Files.createTempDirectory("unittest");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        applicationContext, "cluster.storage.root-path=" + tempDirectory.toString());

    TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
        applicationContext,
        "cluster.tables.allowed-client-name-values="
            + String.join(",", testAllowedClientNameValues));
  }
}
