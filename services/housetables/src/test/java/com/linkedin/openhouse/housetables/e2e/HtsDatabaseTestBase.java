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
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.tidb.TiDBContainer;

/**
 * Points every HTS Spring test context at one disposable database container per test JVM, built
 * from the canonical {@code services/housetables/ddl} files. Gradle's {@code -PhtsBackend} selects
 * MySQL (the default) or TiDB; both speak the MySQL protocol and run through the service's MYSQL
 * datasource path. There is deliberately no H2 or cross-backend fallback: a missing Docker daemon
 * or a failed startup fails the run.
 *
 * <p>Each test starts from the same store: the four HTS tables are emptied and the test {@code
 * data.sql} toggle rules are loaded before every method, and the tables are emptied again after it.
 * The reset is raw SQL through the real service datasource, so it never hydrates a corrupt
 * discriminator and works unchanged in the mock-application contexts, whose datasource is real.
 */
public abstract class HtsDatabaseTestBase {

  public static final String DATABASE = "hts_test";

  private static final String MYSQL_IMAGE = "mysql:8.4.11";

  /**
   * pingcap/tidb v8.5.4, pinned by its multi-architecture index digest. Testcontainers 1.21.4
   * rejects a combined tag and digest, so the version is enforced at runtime instead.
   */
  private static final String TIDB_IMAGE =
      "pingcap/tidb@sha256:e986dadd59937fbfcbdfe7fd084421cc65988da8b795f75194b05a0292635ead";

  private static final int TIDB_PORT = 4000;

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
    private static final String BACKEND = selectedBackend();

    private static final JdbcDatabaseContainer<?> CONTAINER =
        TIDB.equals(BACKEND) ? startTidb() : startMysql();

    /** The one endpoint shared by Spring and the inspection connections. */
    private static final String JDBC_URL =
        TIDB.equals(BACKEND)
            ? "jdbc:mysql://"
                + CONTAINER.getHost()
                + ":"
                + CONTAINER.getMappedPort(TIDB_PORT)
                + "/"
                + DATABASE
                + "?useSSL=false&allowPublicKeyRetrieval=true"
            : CONTAINER.getJdbcUrl();

    static {
      try (Connection connection =
          DriverManager.getConnection(JDBC_URL, CONTAINER.getUsername(), CONTAINER.getPassword())) {
        System.out.println(
            "HTS test backend: "
                + BACKEND
                + " image="
                + CONTAINER.getDockerImageName()
                + " server="
                + connection.getMetaData().getDatabaseProductVersion()
                + " driver="
                + connection.getMetaData().getDriverVersion());
      } catch (SQLException e) {
        throw new IllegalStateException(
            "Could not connect to the HTS " + BACKEND + " test database", e);
      }
    }

    private static MySQLContainer<?> startMysql() {
      MySQLContainer<?> mysql =
          new MySQLContainer<>(MYSQL_IMAGE)
              .withDatabaseName(DATABASE)
              .withCommand(
                  "--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci");
      start(mysql, MYSQL_IMAGE);
      try (Connection connection =
          DriverManager.getConnection(
              mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
        populate(connection);
      } catch (SQLException e) {
        throw new IllegalStateException("Could not build the HTS MySQL test schema", e);
      }
      return mysql;
    }

    /**
     * Single-process unistore TiDB. Its server default collation is utf8mb4_bin, so the test
     * database is created with the canonical collation that the DDL's job and toggle tables
     * inherit.
     */
    private static TiDBContainer startTidb() {
      TiDBContainer tidb =
          new TiDBContainer(TIDB_IMAGE)
              .withCommand(
                  "--store=unistore",
                  "--path=",
                  "--host=0.0.0.0",
                  "--status-host=0.0.0.0",
                  "-L",
                  "warn");
      start(tidb, TIDB_IMAGE + " (v8.5.4)");
      try (Connection connection = tidb.createConnection("")) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(
              "CREATE DATABASE " + DATABASE + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
        }
        connection.setCatalog(DATABASE);
        populate(connection);
      } catch (SQLException e) {
        throw new IllegalStateException("Could not build the HTS TiDB test schema", e);
      }
      return tidb;
    }

    private static void start(JdbcDatabaseContainer<?> container, String image) {
      try {
        container.start();
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "Could not start the HTS " + BACKEND + " test database from " + image, e);
      }
    }

    private static void populate(Connection connection) {
      ResourceDatabasePopulator schema = new ResourceDatabasePopulator();
      for (String ddl : DDL) {
        schema.addScript(new ClassPathResource(ddl));
      }
      schema.populate(connection);
    }
  }

  @DynamicPropertySource
  static void mysqlProperties(DynamicPropertyRegistry properties) {
    JdbcDatabaseContainer<?> container = Holder.CONTAINER;
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
    JdbcDatabaseContainer<?> container = Holder.CONTAINER;
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
