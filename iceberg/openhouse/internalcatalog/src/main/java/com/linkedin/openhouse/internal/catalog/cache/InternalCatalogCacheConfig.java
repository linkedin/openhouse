package com.linkedin.openhouse.internal.catalog.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class InternalCatalogCacheConfig {

  @Bean(name = "internalCatalogCacheManager")
  public CacheManager internalCatalogCacheManager(InternalCatalogCacheProperties cacheProperties) {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of("tableMetadata"));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(cacheProperties.getTtl())
            .maximumSize(cacheProperties.getMaxSize())
            .recordStats());
    return cacheManager;
  }
}
