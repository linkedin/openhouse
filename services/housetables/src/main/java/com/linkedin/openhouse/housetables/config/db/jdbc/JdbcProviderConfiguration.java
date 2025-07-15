package com.linkedin.openhouse.housetables.config.db.jdbc;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configure JDBC sources such as h2, mysql, postgres
 *
 * <p>Such sources are configured by providing a bean for appropriate {@link DataSource} and then
 * annotating the repository implementation sources with {@link
 * org.springframework.data.jpa.repository.config.EnableJpaRepositories}
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

  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource")
  public DataSourceProperties dataSourceProperties() {
    return new DataSourceProperties();
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
  public DataSource dataSource() {
    DatabaseConfiguration.SupportedDbTypes dbType =
        DatabaseConfiguration.SupportedDbTypes.valueOf(
            clusterProperties.getClusterHouseTablesDatabaseType());

    log.info("Using {} database for HouseTables service", dbType);

    DataSourceProperties properties = dataSourceProperties();

    // Set database-specific properties
    if (dbType == DatabaseConfiguration.SupportedDbTypes.ICEBERG) {
      properties.setUrl(H2_DEFAULT_URL);
    } else {
      properties.setUrl(clusterProperties.getClusterHouseTablesDatabaseUrl());
      properties.setUsername(clusterProperties.getClusterHouseTablesDatabaseUsername());
      properties.setPassword(clusterProperties.getClusterHouseTablesDatabasePassword());
    }

    return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
  }
}
