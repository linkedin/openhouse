package com.linkedin.openhouse.cluster.database;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Builds service-local connection pools for the OpenHouse metadata database.
 *
 * <p>Consumers share the connection identity and TLS configuration from {@link ClusterProperties},
 * but each process owns and sizes its own Hikari pool.
 */
public final class MetadataDataSourceFactory {

  private static final String MYSQL_JDBC_PREFIX = "jdbc:mysql:";

  private MetadataDataSourceFactory() {}

  /** Build a pool using the shared metadata database URL. */
  public static HikariDataSource create(ClusterProperties clusterProperties) {
    return create(clusterProperties, clusterProperties.getClusterMetadataDatabaseUrl());
  }

  /**
   * Build a pool using an explicit URL and the shared credentials and TLS configuration.
   *
   * <p>The URL override supports HTS's in-memory and Iceberg modes without leaking those
   * HTS-specific backend choices into the shared connection configuration.
   */
  public static HikariDataSource create(ClusterProperties clusterProperties, String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
      throw new IllegalStateException(
          "cluster.metadata.database.url must be configured for JDBC metadata storage");
    }

    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(jdbcUrl);
    setIfPresent(dataSource::setUsername, clusterProperties.getClusterMetadataDatabaseUsername());
    setIfPresent(dataSource::setPassword, clusterProperties.getClusterMetadataDatabasePassword());

    if (jdbcUrl.startsWith(MYSQL_JDBC_PREFIX)
        && clusterProperties.isClusterMetadataDatabaseCertBasedAuthEnabled()) {
      configureCertificateAuthentication(dataSource, clusterProperties);
    }
    return dataSource;
  }

  private static void configureCertificateAuthentication(
      HikariDataSource dataSource, ClusterProperties clusterProperties) {
    dataSource.addDataSourceProperty(
        "sslMode", clusterProperties.getClusterMetadataDatabaseCertBasedAuthSslMode());

    addIfPresent(
        dataSource,
        "clientCertificateKeyStoreUrl",
        clusterProperties.getClusterMetadataDatabaseCertBasedAuthClientCertKeystoreUrl());
    addIfPresent(
        dataSource,
        "clientCertificateKeyStorePassword",
        clusterProperties.getClusterMetadataDatabaseCertBasedAuthClientCertKeystorePassword());
    addIfPresent(
        dataSource,
        "trustCertificateKeyStoreUrl",
        clusterProperties.getClusterMetadataDatabaseCertBasedAuthTruststoreUrl());
    addIfPresent(
        dataSource,
        "trustCertificateKeyStorePassword",
        clusterProperties.getClusterMetadataDatabaseCertBasedAuthTruststorePassword());
  }

  private static void addIfPresent(HikariDataSource dataSource, String key, String value) {
    if (value != null && !value.trim().isEmpty()) {
      dataSource.addDataSourceProperty(key, value);
    }
  }

  private static void setIfPresent(java.util.function.Consumer<String> setter, String value) {
    if (value != null) {
      setter.accept(value);
    }
  }
}
