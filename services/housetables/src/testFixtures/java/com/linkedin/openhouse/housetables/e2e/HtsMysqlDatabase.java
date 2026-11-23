package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.MySQLContainer;

/** One canonical-DDL MySQL database per test JVM, shared by HTS and Tables integration tests. */
public final class HtsMysqlDatabase {
  private HtsMysqlDatabase() {}

  private static final String MYSQL_IMAGE =
      "mysql:"
          + Objects.requireNonNull(
              System.getProperty("openhouse.test.mysql.version"),
              "openhouse.test.mysql.version is unset; run tests through Gradle");

  private static final class Holder {
    private static final MySQLContainer<?> CONTAINER = startMysql();

    private static MySQLContainer<?> startMysql() {
      MySQLContainer<?> mysql = new MySQLContainer<>(MYSQL_IMAGE).withDatabaseName("hts_test");
      mysql.start();
      try (Connection connection =
          DriverManager.getConnection(
              mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        Resource[] ddl =
            new PathMatchingResourcePatternResolver().getResources("classpath:hts-mysql/ddl/*.sql");
        assertThat(ddl).as("canonical HTS DDL resources").isNotEmpty();
        Arrays.sort(ddl, Comparator.comparing(Resource::getFilename));
        new ResourceDatabasePopulator(ddl).populate(connection);
      } catch (SQLException | IOException e) {
        throw new IllegalStateException("Could not build the HTS MySQL test schema", e);
      }
      return mysql;
    }
  }

  public static void configure(DynamicPropertyRegistry properties) {
    MySQLContainer<?> container = Holder.CONTAINER;
    properties.add("cluster.housetables.database.type", () -> "MYSQL");
    properties.add("cluster.housetables.database.url", container::getJdbcUrl);
    properties.add("HTS_DB_USER", container::getUsername);
    properties.add("HTS_DB_PASSWORD", container::getPassword);
    properties.add("cluster.housetables.database.cert-based-auth.enabled", () -> "false");
    properties.add("spring.sql.init.mode", () -> "never");
    properties.add("spring.jpa.hibernate.ddl-auto", () -> "none");
    properties.add("spring.datasource.hikari.minimum-idle", () -> "0");
  }

  public static Connection openConnection() throws SQLException {
    MySQLContainer<?> container = Holder.CONTAINER;
    return DriverManager.getConnection(
        container.getJdbcUrl(), container.getUsername(), container.getPassword());
  }
}
