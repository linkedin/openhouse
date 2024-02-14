package com.linkedin.openhouse.housetables.config.db;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.iceberg.JobTableHtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.iceberg.UserTableHtsRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.JobTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Configure the storage to be used for repositories in /hts
 *
 * <p>It plugs in the appropriate repository from the available beans such as - Iceberg based
 * repository (iceberg) - Jdbc based repository (h2, mysql, postgres, mysql server) - Other
 * repositories in the future (mongodb, espresso)
 */
@Configuration
@EnableJpaRepositories(basePackages = "com.linkedin.openhouse.housetables.repository.impl.jdbc")
public class DatabaseConfiguration {

  public enum SupportedDbTypes {
    IN_MEMORY,
    MYSQL,
    ICEBERG;
  }

  @Autowired ClusterProperties clusterProperties;

  /** jdbc based implementations * */
  @Autowired JobTableHtsJdbcRepository jobTableHtsJdbcRepository;

  @Autowired UserTableHtsJdbcRepository userTableHtsJdbcRepository;

  /** iceberg based implementations * */
  @Autowired(required = false)
  JobTableHtsRepository jobTableHtsIcebergRepository;

  @Autowired(required = false)
  UserTableHtsRepository userTableHtsIcebergRepository;

  /** provide appropriate implementations * */
  @Primary
  @Bean
  public HtsRepository<JobRow, JobRowPrimaryKey> jobHtsRepositoryPrimary() {
    switch (SupportedDbTypes.valueOf(clusterProperties.getClusterHouseTablesDatabaseType())) {
      case ICEBERG:
        return jobTableHtsIcebergRepository;
      default:
        return jobTableHtsJdbcRepository;
    }
  }

  @Primary
  @Bean
  public HtsRepository<UserTableRow, UserTableRowPrimaryKey> userTableHtsRepositoryPrimary() {
    switch (SupportedDbTypes.valueOf(clusterProperties.getClusterHouseTablesDatabaseType())) {
      case ICEBERG:
        return userTableHtsIcebergRepository;
      default:
        return userTableHtsJdbcRepository;
    }
  }
}
