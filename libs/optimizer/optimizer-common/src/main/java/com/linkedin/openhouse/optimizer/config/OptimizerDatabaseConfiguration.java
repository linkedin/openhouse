package com.linkedin.openhouse.optimizer.config;

import com.linkedin.openhouse.cluster.configs.YamlPropertySourceFactory;
import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseProperties.CertBasedAuth;
import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseProperties.DatabaseType;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/** HTS-style JDBC configuration shared only by the optimizer service and batch applications. */
@Configuration
@PropertySource(
    name = "optimizerCluster",
    value = "file:${OPENHOUSE_CLUSTER_CONFIG_PATH:/var/config/cluster.yaml}",
    factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
@EnableConfigurationProperties({DataSourceProperties.class, OptimizerDatabaseProperties.class})
@Slf4j
public class OptimizerDatabaseConfiguration {

  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource.hikari")
  public HikariDataSource optimizerDataSource(
      DataSourceProperties dataSourceProperties, OptimizerDatabaseProperties databaseProperties) {
    if (databaseProperties.getUrl() != null) {
      Assert.hasText(
          databaseProperties.getUrl(), "cluster.optimizer.database.url must not be blank");
      // Override before building so the driver is inferred from the effective URL, not the
      // application's legacy default (MySQL for the service, H2 for the batch applications).
      dataSourceProperties.setUrl(databaseProperties.getUrl());
    }

    DatabaseDriver driver = DatabaseDriver.fromJdbcUrl(dataSourceProperties.determineUrl());
    if (databaseProperties.getType() != null) {
      DatabaseDriver expectedDriver =
          databaseProperties.getType() == DatabaseType.MYSQL
              ? DatabaseDriver.MYSQL
              : DatabaseDriver.H2;
      Assert.isTrue(
          driver == expectedDriver, "cluster.optimizer.database.type must match the JDBC URL");
    }

    CertBasedAuth certBasedAuth = databaseProperties.getCertBasedAuth();
    if (certBasedAuth.isEnabled()) {
      Assert.isTrue(
          driver == DatabaseDriver.MYSQL,
          "cluster.optimizer.database.cert-based-auth requires a MySQL JDBC URL");
      Assert.hasText(
          certBasedAuth.getSslMode(),
          "cluster.optimizer.database.cert-based-auth.ssl-mode must not be blank");
    }

    log.info("Using {} database for optimizer", driver);
    HikariDataSource dataSource =
        dataSourceProperties.initializeDataSourceBuilder().type(HikariDataSource.class).build();

    if (certBasedAuth.isEnabled()) {
      dataSource.addDataSourceProperty("sslMode", certBasedAuth.getSslMode());
      if (StringUtils.hasText(certBasedAuth.getClientCertKeystoreUrl())) {
        dataSource.addDataSourceProperty(
            "clientCertificateKeyStoreUrl", certBasedAuth.getClientCertKeystoreUrl());
        if (certBasedAuth.getClientCertKeystorePassword() != null) {
          dataSource.addDataSourceProperty(
              "clientCertificateKeyStorePassword", certBasedAuth.getClientCertKeystorePassword());
        }
      }
      if (StringUtils.hasText(certBasedAuth.getTruststoreUrl())) {
        dataSource.addDataSourceProperty(
            "trustCertificateKeyStoreUrl", certBasedAuth.getTruststoreUrl());
        if (certBasedAuth.getTruststorePassword() != null) {
          dataSource.addDataSourceProperty(
              "trustCertificateKeyStorePassword", certBasedAuth.getTruststorePassword());
        }
      }
      log.info(
          "Configured optimizer MySQL certificate-based authentication (sslMode={})",
          certBasedAuth.getSslMode());
    }

    return dataSource;
  }
}
