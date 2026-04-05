package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.cache.InternalCatalogCacheProperties;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class InternalCatalogCachePropertiesConfigTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(InternalCatalogCachePropertiesConfig.class);

  @Test
  public void testDefaultMetadataCacheProperties() {
    contextRunner.run(
        context -> assertMetadataCacheProperties(context, Duration.ofMinutes(5), 1000));
  }

  @Test
  public void testOverriddenMetadataCacheProperties() {
    contextRunner
        .withPropertyValues(
            "cluster.iceberg.tables.metadata-cache.ttl=7m",
            "cluster.iceberg.tables.metadata-cache.max-size=42")
        .run(context -> assertMetadataCacheProperties(context, Duration.ofMinutes(7), 42));
  }

  private void assertMetadataCacheProperties(
      AssertableApplicationContext context, Duration expectedTtl, long expectedMaxSize) {
    Assertions.assertNull(context.getStartupFailure());

    InternalCatalogCacheProperties cacheProperties =
        context.getBean(InternalCatalogCacheProperties.class);
    Assertions.assertEquals(expectedTtl, cacheProperties.getTtl());
    Assertions.assertEquals(expectedMaxSize, cacheProperties.getMaxSize());
    Assertions.assertFalse(context.containsBean("internalCatalogCacheManager"));
  }
}
