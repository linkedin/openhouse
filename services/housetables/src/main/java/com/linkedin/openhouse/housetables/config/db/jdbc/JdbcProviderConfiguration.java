package com.linkedin.openhouse.housetables.config.db.jdbc;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.DatabaseConfiguration;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

  @Autowired private ClusterProperties clusterProperties;

  /**
   * jdbc url is database specific. Here an "H2" database is chosen to work with in-"mem"ory mode on
   * "htsdb" database. With DB_CLOSE_DELAY=-1, the database is kept alive as long as the JVM lives,
   * otherwise it shuts down when the database-creating-thread dies.
   */
  private static final String H2_DEFAULT_URL = "jdbc:h2:mem:htsdb;DB_CLOSE_DELAY=-1";

  @Bean
  public DataSource provideJdbcDataSource() {
    DatabaseConfiguration.SupportedDbTypes dbType =
        DatabaseConfiguration.SupportedDbTypes.valueOf(
            clusterProperties.getClusterHouseTablesDatabaseType());

    log.info(String.format("Using %s database for HouseTables service", dbType));

    //  if storage type is Iceberg, use H2 as the default jdbc database
    return DataSourceBuilder.create()
        .url(
            dbType == DatabaseConfiguration.SupportedDbTypes.ICEBERG
                ? H2_DEFAULT_URL
                : clusterProperties.getClusterHouseTablesDatabaseUrl())
        .username(clusterProperties.getClusterHouseTablesDatabaseUsername())
        .password(clusterProperties.getClusterHouseTablesDatabasePassword())
        .build();
  }
}
