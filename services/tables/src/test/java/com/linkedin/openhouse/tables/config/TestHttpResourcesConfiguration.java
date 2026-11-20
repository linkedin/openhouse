package com.linkedin.openhouse.tables.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorResourceFactory;

/** Closing one cached test context must not stop another context's HTS HTTP client. */
@Configuration
public class TestHttpResourcesConfiguration {
  @Bean
  public ReactorResourceFactory reactorResourceFactory() {
    ReactorResourceFactory resources = new ReactorResourceFactory();
    resources.setUseGlobalResources(false);
    return resources;
  }
}
