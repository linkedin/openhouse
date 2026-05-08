package com.linkedin.openhouse.internal.catalog.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.unit.DataSize;

@Getter
@Setter
public class InternalCatalogSettings {

  private MetadataCache metadataCache = new MetadataCache();

  @Getter
  @Setter
  public static class MetadataCache {
    private boolean enabled = false;
    private Duration ttl = Duration.ofMinutes(10);
    private DataSize maxWeight = DataSize.ofGigabytes(2);
  }
}
