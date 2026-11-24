package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

/**
 * Points every HTS Spring test context at one disposable MySQL container per test JVM, built from
 * the canonical {@code services/housetables/ddl} files. There is deliberately no H2 fallback: a
 * missing Docker daemon or a failed startup fails the run.
 *
 * <p>Each test starts from the same store: every table is emptied and the test {@code data.sql}
 * toggle rules are loaded before every method. The reset is raw SQL through the real service
 * datasource, so it never hydrates a corrupt discriminator and works unchanged in mock-application
 * contexts, whose datasource is real.
 */
public abstract class HtsDatabaseTestBase {

  private static final String DATABASE = "hts_test";

  private static final String MYSQL_IMAGE =
      "mysql:"
          + Objects.requireNonNull(
              System.getProperty("openhouse.test.mysql.version"),
              "openhouse.test.mysql.version is unset; run tests through Gradle");

  /** Started and populated once, before any context builds a datasource; never stopped early. */
  private static final class Holder {
    private static final MySQLContainer<?> CONTAINER = startMysql();

    private static final String JDBC_URL = CONTAINER.getJdbcUrl();

    private static MySQLContainer<?> startMysql() {
      MySQLContainer<?> mysql = new MySQLContainer<>(MYSQL_IMAGE).withDatabaseName(DATABASE);
      mysql.start();
      try (Connection connection =
          DriverManager.getConnection(
              mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        populate(connection);
      } catch (SQLException | IOException e) {
        throw new IllegalStateException("Could not build the HTS MySQL test schema", e);
      }
      return mysql;
    }

    private static void populate(Connection connection) throws IOException {
      Resource[] ddl =
          new PathMatchingResourcePatternResolver().getResources("classpath:hts-mysql/ddl/*.sql");
      assertThat(ddl).as("canonical HTS DDL resources").isNotEmpty();
      Arrays.sort(ddl, Comparator.comparing(Resource::getFilename));
      new ResourceDatabasePopulator(ddl).populate(connection);
    }
  }

  @DynamicPropertySource
  static void mysqlProperties(DynamicPropertyRegistry properties) {
    MySQLContainer<?> container = Holder.CONTAINER;
    properties.add("cluster.housetables.database.type", () -> "MYSQL");
    properties.add("cluster.housetables.database.url", () -> Holder.JDBC_URL);
    properties.add("HTS_DB_USER", container::getUsername);
    properties.add("HTS_DB_PASSWORD", container::getPassword);
    properties.add("cluster.housetables.database.cert-based-auth.enabled", () -> "false");
    properties.add("spring.sql.init.mode", () -> "never");
    properties.add("spring.jpa.hibernate.ddl-auto", () -> "none");
    properties.add("spring.datasource.hikari.minimum-idle", () -> "0");
  }

  @Autowired private DataSource mysqlFixtureDataSource;

  @BeforeEach
  public void prepareMysqlFixture() throws SQLException {
    try (Connection connection = mysqlFixtureDataSource.getConnection()) {
      clear(connection);
      ScriptUtils.executeSqlScript(connection, new ClassPathResource("data.sql"));
    }
  }

  /** Resets every table in the disposable HTS database before each test. */
  private static void clear(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet result =
          statement.executeQuery(
              "SELECT table_name FROM information_schema.tables"
                  + " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'")) {
        while (result.next()) {
          tables.add(result.getString(1));
        }
      }
      assertThat(tables).as("HTS test database tables").isNotEmpty();
      for (String table : tables) {
        statement.execute("TRUNCATE TABLE `" + table.replace("`", "``") + "`");
      }
    }
  }
}
