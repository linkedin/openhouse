package com.linkedin.openhouse.tables.config;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@Getter
@Setter
@ConfigurationProperties(prefix = "cluster.iceberg.tables")
public class InternalCatalogProperties {

  private MetadataCache metadataCache = new MetadataCache();

  private Audit audit = new Audit();

  @Getter
  @Setter
  public static class MetadataCache {
    private Boolean enabled;
    private Duration ttl;
    private DataSize maxWeight;
  }

  @Getter
  @Setter
  public static class Audit {
    private List<String> tablePropertiesAllowlist = Collections.emptyList();
  }
}
