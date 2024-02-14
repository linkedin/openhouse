package com.linkedin.openhouse.housetables.config;

import com.linkedin.openhouse.cluster.metrics.TagUtils;
import com.linkedin.openhouse.common.config.BaseApplicationConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Main application configuration to load cluster properties and define required beans */
@Configuration
public class MainApplicationConfig extends BaseApplicationConfig {
  public static final String APP_NAME = "housetables";

  @Bean
  MeterRegistryCustomizer<MeterRegistry> provideMeterRegistry() {
    return registry ->
        registry.config().commonTags(TagUtils.buildCommonTag(clusterProperties, APP_NAME));
  }
}
