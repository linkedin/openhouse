package com.linkedin.openhouse.optimizer.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseConfiguration;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.service.OptimizerDataService;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest(
    classes = SchedulerApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "cluster.optimizer.database.type=IN_MEMORY",
      "cluster.optimizer.database.url=jdbc:h2:mem:scheduler_config;MODE=MySQL;DB_CLOSE_DELAY=-1",
      "cluster.optimizer.database.cert-based-auth.enabled=false",
      "OPTIMIZER_DB_USER=sa",
      "OPTIMIZER_DB_PASSWORD=",
      "spring.datasource.hikari.maximum-pool-size=7",
      "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
      "spring.sql.init.mode=always",
      "spring.jpa.defer-datasource-initialization=true",
      "spring.sql.init.schema-locations=classpath:db/optimizer-schema.sql"
    })
class SchedulerDatabaseConfigurationTest {

  @Autowired ApplicationContext context;
  @Autowired TableOperationsRepository repository;
  @MockBean SchedulerRunner runner;

  @Test
  void wiresOptimizerDataSourceWithoutImportingRestService() {
    assertThat(context.getBean(OptimizerDatabaseConfiguration.class)).isNotNull();
    assertThat(context.getBeansOfType(DataSource.class)).hasSize(1);
    assertThat(context.getBeansOfType(OptimizerDataService.class)).isEmpty();
    HikariDataSource dataSource = context.getBean(HikariDataSource.class);
    assertThat(dataSource.getJdbcUrl())
        .isEqualTo("jdbc:h2:mem:scheduler_config;MODE=MySQL;DB_CLOSE_DELAY=-1");
    assertThat(dataSource.getDriverClassName()).isEqualTo("org.h2.Driver");
    assertThat(dataSource.getUsername()).isEqualTo("sa");
    assertThat(dataSource.getMaximumPoolSize()).isEqualTo(7);
    assertThat(repository.count()).isZero();
  }
}
