package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.internal.catalog.config.InternalCatalogSettings;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(InternalCatalogProperties.class)
public class InternalCatalogBeans {

  @Bean
  public InternalCatalogSettings internalCatalogSettings(InternalCatalogProperties properties) {
    InternalCatalogSettings settings = new InternalCatalogSettings();
    InternalCatalogProperties.MetadataCache metadataCacheOverrides = properties.getMetadataCache();

    if (metadataCacheOverrides != null) {
      if (metadataCacheOverrides.getEnabled() != null) {
        settings.getMetadataCache().setEnabled(metadataCacheOverrides.getEnabled());
      }
      if (metadataCacheOverrides.getTtl() != null) {
        settings.getMetadataCache().setTtl(metadataCacheOverrides.getTtl());
      }
      if (metadataCacheOverrides.getMaxWeight() != null) {
        settings.getMetadataCache().setMaxWeight(metadataCacheOverrides.getMaxWeight());
      }
    }

    return settings;
  }
}
