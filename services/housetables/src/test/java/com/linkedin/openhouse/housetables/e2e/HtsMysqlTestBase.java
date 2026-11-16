package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

/**
 * Points every HTS Spring test context at one disposable MySQL container per test JVM, built from
 * the canonical {@code services/housetables/ddl} files. There is deliberately no H2 fallback: a
 * missing Docker daemon fails the run.
 *
 * <p>Each test starts from the same store: the four HTS tables are emptied and the test {@code
 * data.sql} toggle rules are loaded before every method, and the tables are emptied again after it.
 * The reset is raw SQL through the real service datasource, so it never hydrates a corrupt
 * discriminator and works unchanged in the mock-application contexts, whose datasource is real.
 */
public abstract class HtsMysqlTestBase {

  public static final String DATABASE = "hts_test";

  private static final String IMAGE = "mysql:8.4.11";

  private static final String MYSQL = "mysql";

  private static final String TIDB = "tidb";

  private static final List<String> DDL =
      Arrays.asList(
          "hts-mysql/ddl/0000__baseline.sql",
          "hts-mysql/ddl/0001__add_entity_type_to_user_table_row.sql");

  private static final List<String> TABLES =
      Arrays.asList(
          "user_table_row", "soft_deleted_user_table_row", "job_row", "table_toggle_rule");

  /** Started and populated once, before any context builds a datasource; never stopped early. */
  private static final class Holder {
    private static final MySQLContainer<?> MYSQL = start();

    private static MySQLContainer<?> start() {
      MySQLContainer<?> mysql =
          new MySQLContainer<>(IMAGE)
              .withDatabaseName(DATABASE)
              .withCommand(
                  "--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci");
      mysql.start();
      ResourceDatabasePopulator schema = new ResourceDatabasePopulator();
      for (String ddl : DDL) {
        schema.addScript(new ClassPathResource(ddl));
      }
      try (Connection connection =
          DriverManager.getConnection(
              mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        schema.populate(connection);
      } catch (SQLException e) {
        throw new IllegalStateException("Could not build the HTS MySQL test schema", e);
      }
      return mysql;
    }
  }

  @DynamicPropertySource
  static void mysqlProperties(DynamicPropertyRegistry properties) {
    MySQLContainer<?> mysql = Holder.MYSQL;
    properties.add("cluster.housetables.database.type", () -> "MYSQL");
    properties.add("cluster.housetables.database.url", mysql::getJdbcUrl);
    properties.add("HTS_DB_USER", mysql::getUsername);
    properties.add("HTS_DB_PASSWORD", mysql::getPassword);
    properties.add("cluster.housetables.database.cert-based-auth.enabled", () -> "false");
    properties.add("spring.sql.init.mode", () -> "never");
    properties.add("spring.jpa.hibernate.ddl-auto", () -> "none");
    properties.add("spring.datasource.hikari.minimum-idle", () -> "0");
  }

  /** An unpooled connection to the test database, independent of any service session. */
  protected static Connection openInspectionConnection() throws SQLException {
    MySQLContainer<?> mysql = Holder.MYSQL;
    return DriverManager.getConnection(
        mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
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

  /** The backend forwarded by Gradle's -PhtsBackend; mysql when the property is absent. */
  private static String selectedBackend() {
    String backend = System.getProperty("hts.test.backend", MYSQL);
    if (!MYSQL.equals(backend) && !TIDB.equals(backend)) {
      throw new IllegalStateException(
          "Unsupported hts.test.backend '"
              + backend
              + "'. Supported values are mysql and tidb; select one with -PhtsBackend.");
    }
    return backend;
  }

  /**
   * Both engines report the JDBC product name MySQL, so the server version tells them apart. It
   * pins the exact image version as well as the engine.
   */
  protected static void assertSelectedBackend(Connection connection) throws SQLException {
    String backend = selectedBackend();
    String product = connection.getMetaData().getDatabaseProductName();
    String version = connection.getMetaData().getDatabaseProductVersion();
    String observed = "selected " + backend + ", observed " + product + " " + version;
    assertThat(product).as(observed).isEqualTo("MySQL");
    if (TIDB.equals(backend)) {
      assertThat(version).as(observed).contains("-TiDB-v8.5.4");
    } else {
      assertThat(version).as(observed).contains("8.4.11").doesNotContain("TiDB");
    }
  }

  /** Refuses to delete anything unless the connection is the disposable selected test database. */
  private static void clear(Connection connection) throws SQLException {
    assertSelectedBackend(connection);
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
