package com.linkedin.openhouse.cluster.storage.configs;

import com.linkedin.openhouse.cluster.configs.YamlPropertySourceFactory;
import java.util.HashMap;
import java.util.Map;
import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ConfigurationProperties(prefix = "cluster.storages")
@PropertySource(
    name = "clusterStorage",
    value = "file:${OPENHOUSE_CLUSTER_CONFIG_PATH:/var/config/cluster.yaml}",
    factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
@Getter
@Setter
public class StorageProperties {
  private String defaultType;
  private Map<String, StorageTypeProperties> types;

  @Getter
  @Setter
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder(toBuilder = true)
  public static class StorageTypeProperties {
    private String scheme;
    private String rootPath;
    private String endpoint;
    @Builder.Default private Map<String, String> parameters = new HashMap<>();
  }
}
