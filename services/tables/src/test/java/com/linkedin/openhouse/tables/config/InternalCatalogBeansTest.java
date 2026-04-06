package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.cache.CacheConfiguration;
import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;

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
          assertMetadataCacheOverrides(context, null, null);
          assertMetadataCacheSettings(context, Duration.ofMinutes(5), 1000);
          Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
        });
  }

  @Test
  public void testOverriddenInternalCatalogSettings() {
    contextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-size=42")
        .run(
            context -> {
              assertMetadataCacheOverrides(context, Duration.ofMinutes(7), 42L);
              assertMetadataCacheSettings(context, Duration.ofMinutes(7), 42);
              Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
            });
  }

  @Test
  public void testInternalCatalogSettingsPropagateToCacheConfiguration() {
    crossModuleContextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-size=42")
        .run(
            context -> {
              assertMetadataCacheOverrides(context, Duration.ofMinutes(7), 42L);
              assertMetadataCacheSettings(context, Duration.ofMinutes(7), 42);
              Assertions.assertNotNull(context.getBean(CacheManager.class));
            });
  }

  private void assertMetadataCacheOverrides(
      AssertableApplicationContext context, Duration expectedTtl, Long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogProperties properties = context.getBean(InternalCatalogProperties.class);
    Assertions.assertEquals(expectedTtl, properties.getMetadataCache().getTtl());
    Assertions.assertEquals(expectedMaxSize, properties.getMetadataCache().getMaxSize());
  }

  private void assertMetadataCacheSettings(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogSettings settings = context.getBean(InternalCatalogSettings.class);
    Assertions.assertEquals(expectedTtl, settings.getMetadataCache().getTtl());
    Assertions.assertEquals(expectedMaxSize, settings.getMetadataCache().getMaxSize());
  }
}
