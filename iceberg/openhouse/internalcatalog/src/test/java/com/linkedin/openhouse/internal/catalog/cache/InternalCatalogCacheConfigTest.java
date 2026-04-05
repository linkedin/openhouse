package com.linkedin.openhouse.internal.catalog.cache;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;

class InternalCatalogCacheConfigTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(InternalCatalogCacheConfig.class);

  private final ApplicationContextRunner tableMetadataCacheContextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(InternalCatalogCacheConfig.class)
          .withBean(SpringTableMetadataCache.class, SpringTableMetadataCache::new);

  @Test
  public void testDefaultMetadataCacheConfiguration() {
    contextRunner
        .withBean(InternalCatalogCacheProperties.class, InternalCatalogCacheProperties::new)
        .run(context -> assertMetadataCacheConfiguration(context, Duration.ofMinutes(5), 1000));
  }

  @Test
  public void testConfiguredMetadataCacheConfiguration() {
    contextRunner
        .withBean(
            InternalCatalogCacheProperties.class,
            () -> {
              InternalCatalogCacheProperties cacheProperties = new InternalCatalogCacheProperties();
              cacheProperties.setTtl(Duration.ofMinutes(7));
              cacheProperties.setMaxSize(42);
              return cacheProperties;
            })
        .run(context -> assertMetadataCacheConfiguration(context, Duration.ofMinutes(7), 42));
  }

  @Test
  public void testSpringTableMetadataCacheUsesConfiguredTableMetadataCache() {
    tableMetadataCacheContextRunner
        .withBean(
            InternalCatalogCacheProperties.class,
            () -> {
              InternalCatalogCacheProperties cacheProperties = new InternalCatalogCacheProperties();
              cacheProperties.setTtl(Duration.ofMinutes(7));
              cacheProperties.setMaxSize(42);
              return cacheProperties;
            })
        .run(
            context -> {
              CaffeineCache tableMetadataCache =
                  assertMetadataCacheConfiguration(context, Duration.ofMinutes(7), 42);
              TableMetadataCache cache = context.getBean(TableMetadataCache.class);
              String metadataLocation = "metadata-location";
              TableMetadata seededMetadata = Mockito.mock(TableMetadata.class);
              AtomicInteger loadCount = new AtomicInteger();

              cache.seed(metadataLocation, seededMetadata);
              TableMetadata loadedMetadata =
                  cache.load(
                      metadataLocation,
                      () -> {
                        loadCount.incrementAndGet();
                        return Mockito.mock(TableMetadata.class);
                      });

              Assertions.assertSame(seededMetadata, loadedMetadata);
              Assertions.assertEquals(0, loadCount.get());
              Assertions.assertSame(
                  seededMetadata, tableMetadataCache.get(metadataLocation, TableMetadata.class));
            });
  }

  private CaffeineCache assertMetadataCacheConfiguration(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogCacheProperties cacheProperties =
        context.getBean(InternalCatalogCacheProperties.class);
    Assertions.assertEquals(expectedTtl, cacheProperties.getTtl());
    Assertions.assertEquals(expectedMaxSize, cacheProperties.getMaxSize());

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
    return tableMetadataCache;
  }
}
