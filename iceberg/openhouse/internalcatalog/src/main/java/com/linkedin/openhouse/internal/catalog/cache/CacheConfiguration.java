package com.linkedin.openhouse.internal.catalog.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant;
import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.support.NoOpCacheManager;
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
  public CacheManager internalCatalogCacheManager(
      InternalCatalogSettings settings, ObjectProvider<MeterRegistry> meterRegistry) {
    if (!settings.getMetadataCache().isEnabled()) {
      return new NoOpCacheManager();
    }
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setAllowNullValues(false);
    cacheManager.setCacheNames(List.of("tableMetadata"));
    cacheManager.setCaffeine(
        Caffeine.newBuilder()
            .expireAfterWrite(settings.getMetadataCache().getTtl())
            .maximumWeight(settings.getMetadataCache().getMaxWeight().toBytes())
            .weigher(tableMetadataWeigher())
            .removalListener(removalListener(meterRegistry))
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

  private static RemovalListener<Object, Object> removalListener(
      ObjectProvider<MeterRegistry> meterRegistry) {
    return (key, value, cause) ->
        meterRegistry.ifAvailable(
            registry ->
                registry
                    .counter(
                        InternalCatalogMetricsConstant.METADATA_CACHE_REMOVAL_CTR,
                        InternalCatalogMetricsConstant.CACHE_REMOVAL_CAUSE_TAG,
                        cause.name())
                    .increment());
  }
}
