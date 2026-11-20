package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Points every HTS Spring test context at one disposable MySQL container per test JVM, built from
 * the canonical {@code services/housetables/ddl} files. There is deliberately no H2 fallback: a
 * missing Docker daemon or a failed startup fails the run.
 *
 * <p>Each test starts from the same store: all HTS tables are emptied and the test {@code data.sql}
 * toggle rules are loaded before every method. The reset is raw SQL through the real service
 * datasource, so it never hydrates a corrupt discriminator and works unchanged in mock-application
 * contexts, whose datasource is real.
 */
public abstract class HtsDatabaseTestBase {

  protected static final String DATABASE = "hts_test";

  @DynamicPropertySource
  static void mysqlProperties(DynamicPropertyRegistry properties) {
    HtsMysqlDatabase.configure(properties);
  }

  /** An unpooled connection to the test database, independent of any service session. */
  protected static Connection openInspectionConnection() throws SQLException {
    return HtsMysqlDatabase.openConnection();
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
