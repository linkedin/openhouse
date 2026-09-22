package com.linkedin.openhouse.cluster.configs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

class ClusterPropertiesTest {

  @Test
  void metadataDatabasePropertiesTakePrecedence() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("cluster.metadata.database.url", "jdbc:mysql://metadata/openhouse");
    properties.put("cluster.housetables.database.url", "jdbc:mysql://legacy/openhouse");
    properties.put("cluster.metadata.database.username", "metadata-user");
    properties.put("OPENHOUSE_DB_USER", "environment-user");
    properties.put("HTS_DB_USER", "legacy-user");

    try (AnnotationConfigApplicationContext context = createContext(properties)) {
      ClusterProperties clusterProperties = context.getBean(ClusterProperties.class);

      assertThat(clusterProperties.getClusterMetadataDatabaseUrl())
          .isEqualTo("jdbc:mysql://metadata/openhouse");
      assertThat(clusterProperties.getClusterMetadataDatabaseUsername()).isEqualTo("metadata-user");
    }
  }

  @Test
  void legacyHtsPropertiesRemainSupported() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("cluster.housetables.database.url", "jdbc:mysql://legacy/openhouse");
    properties.put("HTS_DB_USER", "legacy-user");
    properties.put("HTS_DB_PASSWORD", "legacy-password");
    properties.put("cluster.housetables.database.cert-based-auth.enabled", "true");
    properties.put("cluster.housetables.database.cert-based-auth.ssl-mode", "VERIFY_CA");

    try (AnnotationConfigApplicationContext context = createContext(properties)) {
      ClusterProperties clusterProperties = context.getBean(ClusterProperties.class);

      assertThat(clusterProperties.getClusterMetadataDatabaseUrl())
          .isEqualTo("jdbc:mysql://legacy/openhouse");
      assertThat(clusterProperties.getClusterMetadataDatabaseUsername()).isEqualTo("legacy-user");
      assertThat(clusterProperties.getClusterMetadataDatabasePassword())
          .isEqualTo("legacy-password");
      assertThat(clusterProperties.isClusterMetadataDatabaseCertBasedAuthEnabled()).isTrue();
      assertThat(clusterProperties.getClusterMetadataDatabaseCertBasedAuthSslMode())
          .isEqualTo("VERIFY_CA");
      assertThat(clusterProperties.getClusterHouseTablesDatabaseUrl())
          .isEqualTo("jdbc:mysql://legacy/openhouse");
    }
  }

  private AnnotationConfigApplicationContext createContext(Map<String, Object> properties) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context
        .getEnvironment()
        .getPropertySources()
        .addFirst(new MapPropertySource("test-properties", properties));
    context.register(ClusterProperties.class);
    context.refresh();
    return context;
  }
}
