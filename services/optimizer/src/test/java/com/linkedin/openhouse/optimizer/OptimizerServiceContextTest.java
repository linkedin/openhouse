package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseConfiguration;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Validates that the Spring application context loads successfully against the H2 schema. This test
 * exercises schema-SQL-init, JPA entity scanning, and repository wiring.
 */
@SpringBootTest
@ActiveProfiles("test")
class OptimizerServiceContextTest {

  @Autowired ApplicationContext context;

  @Test
  void contextLoads() {
    assertThat(context.getBean(OptimizerDatabaseConfiguration.class)).isNotNull();
    assertThat(context.getBeansOfType(DataSource.class)).hasSize(1);
    HikariDataSource dataSource = context.getBean(HikariDataSource.class);
    assertThat(dataSource.getJdbcUrl())
        .isEqualTo(
            "jdbc:h2:mem:optimizer_test;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    assertThat(dataSource.getDriverClassName()).isEqualTo("org.h2.Driver");
    assertThat(dataSource.getMaximumPoolSize()).isEqualTo(20);
  }
}
