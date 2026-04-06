package com.linkedin.openhouse.internal.catalog.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CacheConfiguration {

  @Bean
  @ConditionalOnMissingBean(InternalCatalogSettings.class)
  public InternalCatalogSettings internalCatalogSettings() {
    return new InternalCatalogSettings();
  }

  @Bean
  public CacheManager internalCatalogCacheManager(InternalCatalogSettings settings) {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of("tableMetadata"));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(settings.getMetadataCache().getTtl())
            .maximumSize(settings.getMetadataCache().getMaxSize())
            .recordStats());
    return cacheManager;
  }
}
