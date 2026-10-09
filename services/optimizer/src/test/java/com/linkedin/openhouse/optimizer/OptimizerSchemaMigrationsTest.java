package com.linkedin.openhouse.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.testing.MySqlContainerInitializer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.containers.MySQLContainer;

/**
 * Starts the service on a database that already holds HouseTables' tables, as the database it
 * shares with HouseTables does. The database must end up with the tables that Flyway creates in an
 * empty one, and keep HouseTables' table and its rows.
 */
class OptimizerSchemaMigrationsTest {

  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>(MySqlContainerInitializer.MYSQL_IMAGE).withUsername("root");

  /** A stand-in for HouseTables' tables, with a row. */
  private static final String HOUSE_TABLES_TABLE = "user_table_row";

  private static final String[] HOUSE_TABLES_SQL = {
    "CREATE TABLE user_table_row (database_id VARCHAR(128) NOT NULL,"
        + " table_id VARCHAR(128) NOT NULL, version VARCHAR(512), PRIMARY KEY (database_id, table_id))",
    "INSERT INTO user_table_row VALUES ('db', 'tbl', 'v1')"
  };

  /** The tables that Flyway creates in an empty database. */
  private static Map<String, List<String>> emptyDatabaseTables;

  @BeforeAll
  static void migrateEmptyDatabase() throws SQLException {
    MYSQL.start();
    createDatabase("new_database");
    startService("new_database");
    emptyDatabaseTables = tables("new_database");
    assertThat(emptyDatabaseTables)
        .containsOnlyKeys(
            "optimizer_schema_history",
            "table_operations",
            "table_operations_history",
            "table_stats",
            "table_stats_history");
  }

  @AfterAll
  static void stopMySql() {
    MYSQL.stop();
  }

  @Test
  void createsTablesBesideHouseTables() throws SQLException {
    String database = "house_tables";
    createDatabase(database);
    try (Connection connection = connect(database);
        Statement statement = connection.createStatement()) {
      for (String sql : HOUSE_TABLES_SQL) {
        statement.execute(sql);
      }
    }
    Map<String, List<String>> tablesBefore = tables(database);
    Map<String, Long> rowsBefore = rowCounts(database);

    startService(database);

    Map<String, List<String>> tables = tables(database);
    assertThat(tables.remove(HOUSE_TABLES_TABLE)).isEqualTo(tablesBefore.get(HOUSE_TABLES_TABLE));
    assertThat(tables).isEqualTo(emptyDatabaseTables);
    assertThat(rowCounts(database)).containsAllEntriesOf(rowsBefore);
  }

  /** Starts the service on the database, which migrates it, and stops the service. */
  private static void startService(String database) {
    new SpringApplicationBuilder(OptimizerServiceApplication.class)
        .web(WebApplicationType.NONE)
        .run(
            "--cluster.optimizer.database.url=" + jdbcUrl(database),
            "--spring.datasource.username=" + MYSQL.getUsername(),
            "--spring.datasource.password=" + MYSQL.getPassword())
        .close();
  }

  /** Each table's SHOW CREATE TABLE, a line per element. */
  private static Map<String, List<String>> tables(String database) throws SQLException {
    Map<String, List<String>> tables = new TreeMap<>();
    try (Connection connection = connect(database);
        Statement statement = connection.createStatement()) {
      for (String table : tableNames(statement)) {
        try (ResultSet result = statement.executeQuery("SHOW CREATE TABLE `" + table + "`")) {
          result.next();
          tables.put(table, Arrays.asList(result.getString(2).split("\n")));
        }
      }
    }
    return tables;
  }

  private static Map<String, Long> rowCounts(String database) throws SQLException {
    Map<String, Long> rowCounts = new TreeMap<>();
    try (Connection connection = connect(database);
        Statement statement = connection.createStatement()) {
      for (String table : tableNames(statement)) {
        try (ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM `" + table + "`")) {
          result.next();
          rowCounts.put(table, result.getLong(1));
        }
      }
    }
    return rowCounts;
  }

  private static List<String> tableNames(Statement statement) throws SQLException {
    List<String> tableNames = new ArrayList<>();
    try (ResultSet result = statement.executeQuery("SHOW TABLES")) {
      while (result.next()) {
        tableNames.add(result.getString(1));
      }
    }
    return tableNames;
  }

  private static void createDatabase(String database) throws SQLException {
    try (Connection connection = connect(MYSQL.getDatabaseName());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
    }
  }

  private static Connection connect(String database) throws SQLException {
    return DriverManager.getConnection(jdbcUrl(database), MYSQL.getUsername(), MYSQL.getPassword());
  }

  private static String jdbcUrl(String database) {
    return "jdbc:mysql://"
        + MYSQL.getHost()
        + ":"
        + MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT)
        + "/"
        + database
        + "?useSSL=false&allowPublicKeyRetrieval=true";
  }
}
