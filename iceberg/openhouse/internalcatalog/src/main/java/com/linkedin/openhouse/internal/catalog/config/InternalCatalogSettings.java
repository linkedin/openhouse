package com.linkedin.openhouse.internal.catalog.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InternalCatalogSettings {

  private MetadataCache metadataCache = new MetadataCache();

  @Getter
  @Setter
  public static class MetadataCache {
    private Duration ttl = Duration.ofMinutes(5);
    private long maxSize = 1000;
  }
}
