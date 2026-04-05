package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.cache.InternalCatalogCacheConfig;
import com.linkedin.openhouse.internal.catalog.cache.InternalCatalogCacheProperties;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;

class InternalCatalogCachePropertiesConfigTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(InternalCatalogCachePropertiesConfig.class);

  private final ApplicationContextRunner crossModuleContextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(
              InternalCatalogCachePropertiesConfig.class, InternalCatalogCacheConfig.class);

  @Test
  public void testDefaultMetadataCacheProperties() {
    contextRunner.run(
        context -> {
          assertMetadataCacheProperties(context, Duration.ofMinutes(5), 1000);
          Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
        });
  }

  @Test
  public void testOverriddenMetadataCacheProperties() {
    contextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-size=42")
        .run(
            context -> {
              assertMetadataCacheProperties(context, Duration.ofMinutes(7), 42);
              Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
            });
  }

  @Test
  public void testMetadataCachePropertiesPropagateToInternalCatalogCacheConfiguration() {
    crossModuleContextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-size=42")
        .run(
            context -> {
              assertMetadataCacheProperties(context, Duration.ofMinutes(7), 42);
              assertMetadataCacheConfiguration(context, Duration.ofMinutes(7), 42);
            });
  }

  private void assertMetadataCacheProperties(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogCacheProperties cacheProperties =
        context.getBean(InternalCatalogCacheProperties.class);
    Assertions.assertEquals(expectedTtl, cacheProperties.getTtl());
    Assertions.assertEquals(expectedMaxSize, cacheProperties.getMaxSize());
  }

  private void assertMetadataCacheConfiguration(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    CaffeineCacheManager cacheManager =
        context.getBean("internalCatalogCacheManager", CaffeineCacheManager.class);
    Assertions.assertFalse(cacheManager.isAllowNullValues());
    Assertions.assertEquals(List.of("tableMetadata"), List.copyOf(cacheManager.getCacheNames()));

    CaffeineCache tableMetadataCache = (CaffeineCache) cacheManager.getCache("tableMetadata");
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
