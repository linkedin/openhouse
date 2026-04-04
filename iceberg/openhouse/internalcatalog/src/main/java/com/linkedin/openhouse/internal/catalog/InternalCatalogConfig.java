package com.linkedin.openhouse.internal.catalog;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.internal.catalog.cache.TableMetadataCaches;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class InternalCatalogConfig {

  private static final String DEFAULT_METADATA_CACHE_TTL = "5m";
  private static final int DEFAULT_METADATA_CACHE_MAX_SIZE = 1000;

  @Bean(name = TableMetadataCaches.CACHE_MANAGER)
  public CacheManager internalCatalogCacheManager(
      ObjectProvider<ClusterProperties> clusterPropertiesProvider) {
    ClusterProperties clusterProperties = clusterPropertiesProvider.getIfAvailable();
    String metadataCacheTtl =
        clusterProperties == null
            ? DEFAULT_METADATA_CACHE_TTL
            : clusterProperties.getClusterIcebergMetadataCacheTtl();
    int metadataCacheMaxSize =
        clusterProperties == null
            ? DEFAULT_METADATA_CACHE_MAX_SIZE
            : clusterProperties.getClusterIcebergMetadataCacheMaxSize();
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of(TableMetadataCaches.TABLE_METADATA));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(DurationStyle.detectAndParse(metadataCacheTtl))
            .maximumSize(metadataCacheMaxSize)
            .recordStats());
    return cacheManager;
  }
}
