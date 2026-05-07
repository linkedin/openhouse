package com.linkedin.openhouse.internal.catalog.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import java.util.List;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
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
            .maximumWeight(settings.getMetadataCache().getMaxWeight().toBytes())
            .weigher(tableMetadataWeigher())
            .recordStats());
    return cacheManager;
  }

  private static Weigher<Object, Object> tableMetadataWeigher() {
    return (key, value) -> {
      if (value instanceof TableMetadata) {
        return TableMetadataParser.toJson((TableMetadata) value).length();
      }
      return 1;
    };
  }
}
