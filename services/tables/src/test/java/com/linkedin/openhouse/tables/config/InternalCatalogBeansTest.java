package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.cache.CacheConfiguration;
import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.util.unit.DataSize;

class InternalCatalogBeansTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(InternalCatalogBeans.class);

  private final ApplicationContextRunner crossModuleContextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(InternalCatalogBeans.class, CacheConfiguration.class);

  @Test
  public void testDefaultInternalCatalogSettings() {
    contextRunner.run(
        context -> {
          assertMetadataCacheOverrides(context, null, null, null);
          assertMetadataCacheSettings(
              context, false, Duration.ofMinutes(5), DataSize.ofGigabytes(2));
          Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
        });
  }

  @Test
  public void testOverriddenInternalCatalogSettings() {
    contextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.enabled=true",
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-weight=42MB")
        .run(
            context -> {
              assertMetadataCacheOverrides(
                  context, true, Duration.ofMinutes(7), DataSize.ofMegabytes(42));
              assertMetadataCacheSettings(
                  context, true, Duration.ofMinutes(7), DataSize.ofMegabytes(42));
              Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
            });
  }

  @Test
  public void testDisabledMetadataCachePropagatesAsNoOp() {
    crossModuleContextRunner.run(
        context -> {
          assertMetadataCacheSettings(
              context, false, Duration.ofMinutes(5), DataSize.ofGigabytes(2));
          Assertions.assertTrue(context.getBean(CacheManager.class) instanceof NoOpCacheManager);
        });
  }

  @Test
  public void testEnabledMetadataCachePropagatesToCacheConfiguration() {
    crossModuleContextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.enabled=true",
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-weight=42MB")
        .run(
            context -> {
              assertMetadataCacheOverrides(
                  context, true, Duration.ofMinutes(7), DataSize.ofMegabytes(42));
              assertMetadataCacheSettings(
                  context, true, Duration.ofMinutes(7), DataSize.ofMegabytes(42));
              Assertions.assertTrue(
                  context.getBean(CacheManager.class) instanceof CaffeineCacheManager);
            });
  }

  private void assertMetadataCacheOverrides(
      AssertableApplicationContext context,
      Boolean expectedEnabled,
      Duration expectedTtl,
      DataSize expectedMaxWeight) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogProperties properties = context.getBean(InternalCatalogProperties.class);
    Assertions.assertEquals(expectedEnabled, properties.getMetadataCache().getEnabled());
    Assertions.assertEquals(expectedTtl, properties.getMetadataCache().getTtl());
    Assertions.assertEquals(expectedMaxWeight, properties.getMetadataCache().getMaxWeight());
  }

  private void assertMetadataCacheSettings(
      AssertableApplicationContext context,
      boolean expectedEnabled,
      Duration expectedTtl,
      DataSize expectedMaxWeight) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogSettings settings = context.getBean(InternalCatalogSettings.class);
    Assertions.assertEquals(expectedEnabled, settings.getMetadataCache().isEnabled());
    Assertions.assertEquals(expectedTtl, settings.getMetadataCache().getTtl());
    Assertions.assertEquals(expectedMaxWeight, settings.getMetadataCache().getMaxWeight());
  }
}
