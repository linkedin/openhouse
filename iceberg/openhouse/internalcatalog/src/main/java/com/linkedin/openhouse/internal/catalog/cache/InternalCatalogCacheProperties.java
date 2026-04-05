package com.linkedin.openhouse.internal.catalog.cache;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InternalCatalogCacheProperties {

  private Duration ttl = Duration.ofMinutes(5);
  private long maxSize = 1000;
}
