package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
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
 * <p>Each test starts from the same store: the four HTS tables are emptied and the test {@code
 * data.sql} toggle rules are loaded before every method, and the tables are emptied again after it.
 * The reset is raw SQL through the real service datasource, so it never hydrates a corrupt
 * discriminator and works unchanged in mock-application contexts, whose datasource is real.
 */
public abstract class HtsDatabaseTestBase {

  public static final String DATABASE = "hts_test";

  private static final String MYSQL_IMAGE = "mysql:8.4.11";

  private static final List<String> TABLES =
      Arrays.asList(
          "user_table_row", "soft_deleted_user_table_row", "job_row", "table_toggle_rule");

  /** Started and populated once, before any context builds a datasource; never stopped early. */
  private static final class Holder {
    private static final MySQLContainer<?> CONTAINER = startMysql();

    private static final String JDBC_URL = CONTAINER.getJdbcUrl();

    static {
      try (Connection connection =
          DriverManager.getConnection(JDBC_URL, CONTAINER.getUsername(), CONTAINER.getPassword())) {
        System.out.println(
            "HTS test database: image="
                + CONTAINER.getDockerImageName()
                + " server="
                + connection.getMetaData().getDatabaseProductVersion()
                + " driver="
                + connection.getMetaData().getDriverVersion());
      } catch (SQLException e) {
        throw new IllegalStateException("Could not connect to the HTS MySQL test database", e);
      }
    }

    private static MySQLContainer<?> startMysql() {
      MySQLContainer<?> mysql =
          new MySQLContainer<>(MYSQL_IMAGE)
              .withDatabaseName(DATABASE)
              .withCommand(
                  "--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci");
      try {
        mysql.start();
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "Could not start the HTS MySQL test database from " + MYSQL_IMAGE, e);
      }
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

  /** An unpooled connection to the test database, independent of any service session. */
  protected static Connection openInspectionConnection() throws SQLException {
    MySQLContainer<?> container = Holder.CONTAINER;
    return DriverManager.getConnection(
        Holder.JDBC_URL, container.getUsername(), container.getPassword());
  }

  @Autowired private DataSource mysqlFixtureDataSource;

  @BeforeEach
  public void prepareMysqlFixture() throws SQLException {
    try (Connection connection = mysqlFixtureDataSource.getConnection()) {
      clear(connection);
      ScriptUtils.executeSqlScript(connection, new ClassPathResource("data.sql"));
    }
  }

  @AfterEach
  public void clearMysqlFixture() throws SQLException {
    try (Connection connection = mysqlFixtureDataSource.getConnection()) {
      clear(connection);
    }
  }

  /** Refuses to delete anything unless the connection is the disposable HTS test database. */
  private static void clear(Connection connection) throws SQLException {
    assertThat(connection.getAutoCommit()).as("fixture reset runs outside transactions").isTrue();
    try (Statement statement = connection.createStatement()) {
      try (ResultSet database = statement.executeQuery("SELECT DATABASE()")) {
        assertThat(database.next()).isTrue();
        assertThat(database.getString(1)).as("fixture reset database").isEqualTo(DATABASE);
      }
      for (String table : TABLES) {
        statement.executeUpdate("DELETE FROM " + table);
      }
    }
  }
}
