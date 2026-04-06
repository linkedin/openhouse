package com.linkedin.openhouse.tables.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "cluster.iceberg.tables")
public class InternalCatalogProperties {

  private MetadataCache metadataCache = new MetadataCache();

  @Getter
  @Setter
  public static class MetadataCache {
    private Duration ttl;
    private Long maxSize;
  }
}
