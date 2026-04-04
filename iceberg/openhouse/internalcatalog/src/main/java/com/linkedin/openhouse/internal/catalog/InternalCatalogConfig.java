package com.linkedin.openhouse.internal.catalog;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.openhouse.cluster.configs.YamlPropertySourceFactory;
import com.linkedin.openhouse.internal.catalog.cache.TableMetadataCaches;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@EnableCaching
@EnableConfigurationProperties(InternalCatalogProperties.class)
@PropertySource(
    name = "internalCatalogCluster",
    value = "file:${OPENHOUSE_CLUSTER_CONFIG_PATH:/var/config/cluster.yaml}",
    factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
public class InternalCatalogConfig {

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
