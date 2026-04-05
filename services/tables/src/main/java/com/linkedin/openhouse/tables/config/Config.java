package com.linkedin.openhouse.tables.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "cluster.iceberg.tables")
@Getter
@Setter
public class Config {

  private MetadataCacheProperties metadataCache = new MetadataCacheProperties();

  @Bean(name = "internalCatalogCacheManager")
  public CacheManager internalCatalogCacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of("tableMetadata"));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(metadataCache.getTtl())
            .maximumSize(metadataCache.getMaxSize())
            .recordStats());
    return cacheManager;
  }

  @Getter
  @Setter
  public static class MetadataCacheProperties {
    private Duration ttl = Duration.ofMinutes(5);
    private long maxSize = 1000;
  }
}
