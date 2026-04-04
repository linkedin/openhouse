package com.linkedin.openhouse.tables.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.openhouse.internal.catalog.InternalCatalogProperties;
import com.linkedin.openhouse.internal.catalog.cache.TableMetadataCaches;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
@EnableConfigurationProperties(InternalCatalogProperties.class)
public class InternalCatalogCacheConfig {

  @Bean(name = TableMetadataCaches.CACHE_MANAGER)
  public CacheManager internalCatalogCacheManager(
      InternalCatalogProperties internalCatalogProperties) {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of(TableMetadataCaches.TABLE_METADATA));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(internalCatalogProperties.getTtl())
            .maximumSize(internalCatalogProperties.getMaxSize())
            .recordStats());
    return cacheManager;
  }
}
