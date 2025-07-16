package com.linkedin.openhouse.housetables.config.db.jdbc;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configure JDBC sources such as h2, mysql, postgres
 *
 * <p>Spring Boot will automatically create and configure HikariCP DataSource based on
 * spring.datasource.* properties. Additional HikariCP-specific settings can be configured using
 * spring.datasource.hikari.* properties.
 *
 * @see <a href="https://howtodoinjava.com/spring-boot2/datasource-configuration/">Datasource
 *     Configuration</a>
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
  public DataSourceProperties dataSourceProperties() {
    DatabaseConfiguration.SupportedDbTypes dbType =
        DatabaseConfiguration.SupportedDbTypes.valueOf(
            clusterProperties.getClusterHouseTablesDatabaseType());

    log.info("Using {} database for HouseTables service", dbType);

    DataSourceProperties properties = new DataSourceProperties();

    // Set database-specific properties
    properties.setUrl(
        dbType == DatabaseConfiguration.SupportedDbTypes.ICEBERG
            ? H2_DEFAULT_URL
            : clusterProperties.getClusterHouseTablesDatabaseUrl());
    properties.setUsername(clusterProperties.getClusterHouseTablesDatabaseUsername());
    properties.setPassword(clusterProperties.getClusterHouseTablesDatabasePassword());

    return properties;
  }
}
