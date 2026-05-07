package com.linkedin.openhouse.internal.catalog.cache;

import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.springframework.util.unit.DataSize;

class CacheConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(CacheConfiguration.class);

  private final ApplicationContextRunner tableMetadataCacheContextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(CacheConfiguration.class)
          .withBean(SpringTableMetadataCache.class, SpringTableMetadataCache::new)
          .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

  @Test
  public void testDefaultMetadataCacheConfiguration() {
    contextRunner
        .withBean(InternalCatalogSettings.class, InternalCatalogSettings::new)
        .run(
            context ->
                assertMetadataCacheConfiguration(
                    context, Duration.ofMinutes(5), DataSize.ofGigabytes(1)));
  }

  @Test
  public void testConfiguredMetadataCacheConfiguration() {
    contextRunner
        .withBean(
            InternalCatalogSettings.class,
            () -> buildSettings(Duration.ofMinutes(7), DataSize.ofMegabytes(42)))
        .run(
            context ->
                assertMetadataCacheConfiguration(
                    context, Duration.ofMinutes(7), DataSize.ofMegabytes(42)));
  }

  @Test
  public void testSpringTableMetadataCacheUsesConfiguredTableMetadataCache() {
    tableMetadataCacheContextRunner
        .withBean(
            InternalCatalogSettings.class,
            () -> buildSettings(Duration.ofMinutes(7), DataSize.ofMegabytes(42)))
        .run(
            context -> {
              CaffeineCache tableMetadataCache =
                  assertMetadataCacheConfiguration(
                      context, Duration.ofMinutes(7), DataSize.ofMegabytes(42));
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

  private InternalCatalogSettings buildSettings(Duration ttl, DataSize maxWeight) {
    InternalCatalogSettings settings = new InternalCatalogSettings();
    settings.getMetadataCache().setTtl(ttl);
    settings.getMetadataCache().setMaxWeight(maxWeight);
    return settings;
  }

  private CaffeineCache assertMetadataCacheConfiguration(
      AssertableApplicationContext context, Duration expectedTtl, DataSize expectedMaxWeight) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogSettings settings = context.getBean(InternalCatalogSettings.class);
    Assertions.assertEquals(expectedTtl, settings.getMetadataCache().getTtl());
    Assertions.assertEquals(expectedMaxWeight, settings.getMetadataCache().getMaxWeight());

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
        expectedMaxWeight.toBytes(), nativeCache.policy().eviction().orElseThrow().getMaximum());
    Assertions.assertTrue(nativeCache.policy().eviction().orElseThrow().isWeighted());
    return tableMetadataCache;
  }
}
