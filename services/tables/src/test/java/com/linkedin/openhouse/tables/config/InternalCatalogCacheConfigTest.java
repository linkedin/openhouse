package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.InternalCatalogProperties;
import com.linkedin.openhouse.internal.catalog.cache.TableMetadataCaches;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;

class InternalCatalogCacheConfigTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(InternalCatalogCacheConfig.class);

  @Test
  public void testDefaultMetadataCacheProperties() {
    contextRunner.run(
        context -> assertMetadataCacheConfiguration(context, Duration.ofMinutes(5), 1000));
  }

  @Test
  public void testOverriddenMetadataCacheProperties() {
    contextRunner
        .withPropertyValues(
            "cluster.iceberg.metadata-cache.ttl=7m", "cluster.iceberg.metadata-cache.max-size=42")
        .run(context -> assertMetadataCacheConfiguration(context, Duration.ofMinutes(7), 42));
  }

  private void assertMetadataCacheConfiguration(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogProperties internalCatalogProperties =
        context.getBean(InternalCatalogProperties.class);
    Assertions.assertEquals(expectedTtl, internalCatalogProperties.getTtl());
    Assertions.assertEquals(expectedMaxSize, internalCatalogProperties.getMaxSize());

    CaffeineCacheManager cacheManager =
        context.getBean(TableMetadataCaches.CACHE_MANAGER, CaffeineCacheManager.class);
    Assertions.assertEquals(
        List.of(TableMetadataCaches.TABLE_METADATA), List.copyOf(cacheManager.getCacheNames()));

    CaffeineCache tableMetadataCache =
        (CaffeineCache) cacheManager.getCache(TableMetadataCaches.TABLE_METADATA);
    Assertions.assertNotNull(tableMetadataCache);

    com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache =
        tableMetadataCache.getNativeCache();
    Assertions.assertEquals(
        expectedTtl.toNanos(),
        nativeCache
            .policy()
            .expireAfterWrite()
            .orElseThrow()
            .getExpiresAfter(TimeUnit.NANOSECONDS));
    Assertions.assertEquals(
        expectedMaxSize, nativeCache.policy().eviction().orElseThrow().getMaximum());
  }
}
