package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedOrphanFilesDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedStatsCollectionAnalyzer;
import com.linkedin.openhouse.optimizer.testing.MySqlContainerInitializer;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

/**
 * Validates the service's datasource defaults and Spring application context, including the Flyway
 * migrations and repository wiring.
 */
@SpringBootTest
@ContextConfiguration(initializers = MySqlContainerInitializer.class)
class OptimizerServiceContextTest {

  @Autowired ApplicationContext context;

  @Test
  void contextLoads() {
    assertThat(context.getBeansOfType(DataSource.class)).hasSize(1);
    HikariDataSource dataSource = context.getBean(HikariDataSource.class);
    assertThat(dataSource.getJdbcUrl())
        .isEqualTo(context.getEnvironment().getProperty("spring.datasource.url"));
    assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
    assertThat(dataSource.getMaximumPoolSize()).isEqualTo(20);
  }

  @Test
  void statsCollectionAnalyzerEnabled_orphanFilesDeletionDisabled_byDefault() {
    // Staged rollout: stats collection ships first (analyzer.stats.enabled default true); OFD is
    // gated off (analyzer.ofd.enabled default false) so its bean is not registered.
    assertThat(context.getBeansOfType(CadenceBasedStatsCollectionAnalyzer.class)).isNotEmpty();
    assertThat(context.getBeansOfType(CadenceBasedOrphanFilesDeletionAnalyzer.class)).isEmpty();
  }
}
