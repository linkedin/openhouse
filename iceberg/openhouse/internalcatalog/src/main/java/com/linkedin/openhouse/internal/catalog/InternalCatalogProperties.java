package com.linkedin.openhouse.internal.catalog;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed configuration for internal catalog metadata caching. */
@Getter
@Setter
@ConfigurationProperties(prefix = "cluster.iceberg.metadata-cache")
public class InternalCatalogProperties {

  private Duration ttl = Duration.ofMinutes(5);
  private long maxSize = 1000;
}
