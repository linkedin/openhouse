package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.cache.RequestScopedCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InternalCatalogConfig {

  @Bean
  public RequestScopedCache provideRequestScopedCache() {
    return new RequestScopedCache();
  }
}
