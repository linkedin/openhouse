package com.linkedin.openhouse.cluster.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

class MetadataDataSourceFactoryTest {

  @Test
  void createUsesSharedConnectionIdentity() {
    ClusterProperties properties = mock(ClusterProperties.class);
    when(properties.getClusterMetadataDatabaseUrl()).thenReturn("jdbc:mysql://db/openhouse");
    when(properties.getClusterMetadataDatabaseUsername()).thenReturn("openhouse");
    when(properties.getClusterMetadataDatabasePassword()).thenReturn("password");

    try (HikariDataSource dataSource = MetadataDataSourceFactory.create(properties)) {
      assertThat(dataSource.getJdbcUrl()).isEqualTo("jdbc:mysql://db/openhouse");
      assertThat(dataSource.getUsername()).isEqualTo("openhouse");
      assertThat(dataSource.getPassword()).isEqualTo("password");
    }
  }

  @Test
  void createAddsCertificateAuthenticationForMysql() {
    ClusterProperties properties = mock(ClusterProperties.class);
    when(properties.isClusterMetadataDatabaseCertBasedAuthEnabled()).thenReturn(true);
    when(properties.getClusterMetadataDatabaseCertBasedAuthSslMode()).thenReturn("VERIFY_IDENTITY");
    when(properties.getClusterMetadataDatabaseCertBasedAuthClientCertKeystoreUrl())
        .thenReturn("file:/client.p12");
    when(properties.getClusterMetadataDatabaseCertBasedAuthClientCertKeystorePassword())
        .thenReturn("client-password");
    when(properties.getClusterMetadataDatabaseCertBasedAuthTruststoreUrl())
        .thenReturn("file:/truststore.jks");
    when(properties.getClusterMetadataDatabaseCertBasedAuthTruststorePassword())
        .thenReturn("trust-password");

    try (HikariDataSource dataSource =
        MetadataDataSourceFactory.create(properties, "jdbc:mysql://db/openhouse")) {
      assertThat(dataSource.getDataSourceProperties())
          .containsEntry("sslMode", "VERIFY_IDENTITY")
          .containsEntry("clientCertificateKeyStoreUrl", "file:/client.p12")
          .containsEntry("clientCertificateKeyStorePassword", "client-password")
          .containsEntry("trustCertificateKeyStoreUrl", "file:/truststore.jks")
          .containsEntry("trustCertificateKeyStorePassword", "trust-password");
    }
  }

  @Test
  void createDoesNotAddMysqlTlsPropertiesToOtherJdbcBackends() {
    ClusterProperties properties = mock(ClusterProperties.class);
    when(properties.isClusterMetadataDatabaseCertBasedAuthEnabled()).thenReturn(true);

    try (HikariDataSource dataSource =
        MetadataDataSourceFactory.create(properties, "jdbc:h2:mem:test")) {
      assertThat(dataSource.getDataSourceProperties()).isEmpty();
    }
  }

  @Test
  void createRejectsMissingJdbcUrl() {
    ClusterProperties properties = mock(ClusterProperties.class);

    assertThatThrownBy(() -> MetadataDataSourceFactory.create(properties, " "))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cluster.metadata.database.url");
  }
}
