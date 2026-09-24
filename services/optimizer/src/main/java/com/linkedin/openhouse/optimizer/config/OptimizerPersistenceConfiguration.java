package com.linkedin.openhouse.optimizer.config;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.database.MetadataDataSourceFactory;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Owns optimizer persistence wiring while reusing the OpenHouse metadata database connection
 * profile.
 *
 * <p>Every optimizer process creates its own Hikari pool and retains optimizer-specific entities,
 * repositories, schema, and transaction boundaries.
 */
@Configuration
@Import(ClusterProperties.class)
@EntityScan(basePackages = "com.linkedin.openhouse.optimizer.db")
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.optimizer.repository")
public class OptimizerPersistenceConfiguration {

  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource.hikari")
  public HikariDataSource dataSource(ClusterProperties clusterProperties) {
    return MetadataDataSourceFactory.create(clusterProperties);
  }
}
