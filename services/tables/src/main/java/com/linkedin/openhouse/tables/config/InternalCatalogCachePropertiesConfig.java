package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.cache.InternalCatalogCacheProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class InternalCatalogCachePropertiesConfig {

  @Bean
  @ConfigurationProperties(prefix = "cluster.iceberg.tables.metadata-cache")
  public InternalCatalogCacheProperties internalCatalogCacheProperties() {
    return new InternalCatalogCacheProperties();
  }
}
