package com.linkedin.openhouse.housetables.config.db.jdbc;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.database.MetadataDataSourceFactory;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configures the HTS-local pool using the shared OpenHouse metadata database connection profile.
 *
 * <p>HTS retains its backend selection: IN_MEMORY uses its own H2 database, ICEBERG uses H2 only
 * for the JDBC repositories that still need to initialize, and MYSQL uses the shared connection
 * URL, credentials, and certificate settings.
 */
@Configuration
@Slf4j
public class JdbcProviderConfiguration {

  private final ClusterProperties clusterProperties;

  public JdbcProviderConfiguration(ClusterProperties clusterProperties) {
    this.clusterProperties = clusterProperties;
  }

  /**
   * jdbc url is database specific. Here an "H2" database is chosen to work with in-"mem"ory mode on
   * "htsdb" database. With DB_CLOSE_DELAY=-1, the database is kept alive as long as the JVM lives,
   * otherwise it shuts down when the database-creating-thread dies.
   */
  private static final String H2_DEFAULT_URL = "jdbc:h2:mem:htsdb;MODE=MySQL;DB_CLOSE_DELAY=-1";

  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource.hikari")
  public HikariDataSource dataSource() {
    DatabaseConfiguration.SupportedDbTypes dbType =
        DatabaseConfiguration.SupportedDbTypes.valueOf(
            clusterProperties.getClusterHouseTablesDatabaseType());

    log.info("Using {} database for HouseTables service", dbType);
    String jdbcUrl =
        dbType == DatabaseConfiguration.SupportedDbTypes.MYSQL
            ? clusterProperties.getClusterMetadataDatabaseUrl()
            : H2_DEFAULT_URL;
    return MetadataDataSourceFactory.create(clusterProperties, jdbcUrl);
  }
}
