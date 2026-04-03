package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.cache.RequestScopedCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the default {@link RequestScopedCache} bean from the {@code internal.catalog} package
 * because OpenHouse applications explicitly scan this package but do not consistently scan {@code
 * com.linkedin.openhouse.common.cache}. Keeping bean registration here avoids widening every
 * application/test scan list while preserving the cache implementation as a shared utility in
 * {@code services/common}. This default can still be replaced by another bean, for example via
 * {@code @Primary} in a downstream repo.
 */
@Configuration
public class InternalCatalogConfig {

  @Bean
  public RequestScopedCache provideRequestScopedCache() {
    return new RequestScopedCache();
  }
}
