package com.linkedin.openhouse.housetables.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.housetables.config.db.jdbc.JdbcProviderConfiguration;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class JdbcInitializationTest {
  @ParameterizedTest
  @ValueSource(strings = {"spring.sql.init.mode=never", "spring.sql.init.enabled=false"})
  void disabledInitializationStartsWithoutCreatingTables(String property) {
    ClusterProperties clusterProperties = mock(ClusterProperties.class);
    when(clusterProperties.getClusterHouseTablesDatabaseType()).thenReturn("IN_MEMORY");
    when(clusterProperties.getClusterHouseTablesDatabaseUrl())
        .thenReturn("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
    when(clusterProperties.getClusterHouseTablesDatabaseUsername()).thenReturn("sa");
    when(clusterProperties.getClusterHouseTablesDatabasePassword()).thenReturn("");

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SqlInitializationAutoConfiguration.class))
        .withUserConfiguration(JdbcProviderConfiguration.class)
        .withBean(ClusterProperties.class, () -> clusterProperties)
        .withPropertyValues(property)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              try (Connection connection = context.getBean(DataSource.class).getConnection();
                  ResultSet tables =
                      connection.getMetaData().getTables(null, null, "CATALOG_NAMESPACES", null)) {
                assertThat(tables.next()).isFalse();
              }
            });
  }
}
