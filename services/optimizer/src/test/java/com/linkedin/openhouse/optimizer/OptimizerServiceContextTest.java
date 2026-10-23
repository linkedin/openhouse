package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedOrphanFilesDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.analyzer.CadenceBasedStatsCollectionAnalyzer;
import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseConfiguration;
import com.linkedin.openhouse.optimizer.testing.MySqlContainerInitializer;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
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
  @TempDir Path temporaryDirectory;

  @Test
  void contextLoads() {
    assertThat(context.getBean(OptimizerDatabaseConfiguration.class)).isNotNull();
    assertThat(context.getBeansOfType(DataSource.class)).hasSize(1);
    HikariDataSource dataSource = context.getBean(HikariDataSource.class);
    assertThat(dataSource.getJdbcUrl())
        .isEqualTo(context.getEnvironment().getProperty("cluster.optimizer.database.url"));
    assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
    assertThat(dataSource.getMaximumPoolSize()).isEqualTo(20);
  }

  private ApplicationContextRunner dataSourceContextRunner() {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class))
        .withUserConfiguration(OptimizerDatabaseConfiguration.class)
        .withPropertyValues(
            "OPENHOUSE_CLUSTER_CONFIG_PATH=" + temporaryDirectory.resolve("cluster.yaml"));
  }

  @Test
  void retainsServiceDefaultsWhenClusterFileIsMissing() {
    dataSourceContextRunner()
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:3306/oh_db");
              assertThat(dataSource.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
              assertThat(dataSource.getMaximumPoolSize()).isEqualTo(20);
              assertThat(dataSource.getDataSourceProperties()).isEmpty();
            });
  }

  @Test
  void retainsOptimizerEnvironmentVariablesWithoutUsingHtsSettings() {
    dataSourceContextRunner()
        .withPropertyValues(
            "OPTIMIZER_DB_URL=jdbc:mysql://legacy.invalid:3306/optimizer",
            "OPTIMIZER_DB_USER=optimizer_user",
            "OPTIMIZER_DB_PASSWORD=optimizer_test_password",
            "cluster.housetables.database.url=jdbc:mysql://hts.invalid:3306/hts",
            "HTS_DB_USER=hts_user",
            "HTS_DB_PASSWORD=hts_test_password")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(DataSource.class);
              HikariDataSource dataSource = context.getBean(HikariDataSource.class);
              assertThat(dataSource.getJdbcUrl())
                  .isEqualTo("jdbc:mysql://legacy.invalid:3306/optimizer");
              assertThat(dataSource.getUsername()).isEqualTo("optimizer_user");
              assertThat(dataSource.getPassword()).isEqualTo("optimizer_test_password");
            });
  }

  @Test
  void statsCollectionAnalyzerEnabled_orphanFilesDeletionDisabled_byDefault() {
    // Staged rollout: stats collection ships first (analyzer.stats.enabled default true); OFD is
    // gated off (analyzer.ofd.enabled default false) so its bean is not registered.
    assertThat(context.getBeansOfType(CadenceBasedStatsCollectionAnalyzer.class)).isNotEmpty();
    assertThat(context.getBeansOfType(CadenceBasedOrphanFilesDeletionAnalyzer.class)).isEmpty();
  }
}
