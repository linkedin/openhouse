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
import java.util.Objects;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.MySQLContainer;

/**
 * Starts the service on a database that already holds HouseTables' real schema and seeded rows. The
 * database must end up with the optimizer tables that Flyway creates in an empty one, without
 * changing any HouseTables table definition or row. The service runs as a user that can reach only
 * the optimizer tables, so a migration that touches any other table fails its startup.
 */
class OptimizerSchemaMigrationsTest {

  private static final MySQLContainer<?> MYSQL =
      MySqlContainerInitializer.newContainer().withUsername("root");

  /** The service's MySQL user, granted the optimizer tables and nothing else. */
  private static final String SERVICE_USER = "optimizer";

  private static final String[] OPTIMIZER_TABLES = {
    "optimizer_schema_history",
    "table_operations",
    "table_operations_history",
    "table_stats",
    "table_stats_history"
  };

  private static final String[] HOUSE_TABLES_SEED_SQL = {
    "INSERT INTO user_table_row (database_id, table_id, version, metadata_location) VALUES"
        + " ('db', 'orders', 7, 'hdfs://orders/metadata/7.json'),"
        + " ('db', 'members', 3, NULL)",
    "INSERT INTO job_row (job_id, state, job_name, cluster_id, job_conf) VALUES"
        + " ('job-1', 'SUCCEEDED', 'retention', 'cluster', '{\"type\":\"RETENTION\"}'),"
        + " ('job-2', 'RUNNING', 'compaction', 'cluster', NULL)",
    "INSERT INTO table_toggle_rule (feature, database_pattern, table_pattern) VALUES"
        + " ('retention', 'db', 'orders'), ('compaction', 'db', 'members')",
    "INSERT INTO soft_deleted_user_table_row"
        + " (database_id, table_id, deleted_at_ms, version, metadata_location, purge_after_ms) VALUES"
        + " ('db', 'old_orders', 1730000000000, 2, 'hdfs://old_orders/metadata/2.json', 1731000000000),"
        + " ('db', 'old_members', 1730000000001, 1, NULL, 1731000000001)"
  };

  /** The tables that Flyway creates in an empty database. */
  private static Map<String, List<String>> emptyDatabaseTables;

  @BeforeAll
  static void migrateEmptyDatabase() throws SQLException {
    MYSQL.start();
    createDatabase("new_database");
    startService("new_database");
    emptyDatabaseTables = tables("new_database");
    assertThat(emptyDatabaseTables).containsOnlyKeys(OPTIMIZER_TABLES);
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
      ScriptUtils.executeSqlScript(
          connection,
          new FileSystemResource(
              Objects.requireNonNull(
                  System.getProperty("optimizer.house-tables-schema"), "Run through Gradle")));
      for (String sql : HOUSE_TABLES_SEED_SQL) {
        statement.execute(sql);
      }
    }
    Map<String, List<String>> tablesBefore = tables(database);
    Map<String, List<List<String>>> rowsBefore = new TreeMap<>();
    for (String table : tablesBefore.keySet()) {
      rowsBefore.put(table, rows(database, table));
    }

    startService(database);

    Map<String, List<String>> tables = tables(database);
    for (String table : tablesBefore.keySet()) {
      assertThat(tables.remove(table))
          .as("HouseTables definition: %s", table)
          .isEqualTo(tablesBefore.get(table));
      assertThat(rows(database, table))
          .as("HouseTables rows: %s", table)
          .containsExactlyInAnyOrderElementsOf(rowsBefore.get(table));
    }
    assertThat(tables).isEqualTo(emptyDatabaseTables);
  }

  /** Starts the service on the database, which migrates it, and stops the service. */
  private static void startService(String database) {
    new SpringApplicationBuilder(OptimizerServiceApplication.class)
        .web(WebApplicationType.NONE)
        .run(
            "--spring.datasource.url=" + jdbcUrl(database),
            "--spring.datasource.username=" + SERVICE_USER,
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

  private static List<List<String>> rows(String database, String table) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    try (Connection connection = connect(database);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT * FROM `" + table + "`")) {
      int columns = result.getMetaData().getColumnCount();
      while (result.next()) {
        List<String> row = new ArrayList<>(columns);
        for (int column = 1; column <= columns; column++) {
          row.add(result.getString(column));
        }
        rows.add(row);
      }
    }
    return rows;
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

  /** Creates the database and grants the service user the optimizer tables in it. */
  private static void createDatabase(String database) throws SQLException {
    try (Connection connection = connect(MYSQL.getDatabaseName());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute(
          "CREATE USER IF NOT EXISTS '"
              + SERVICE_USER
              + "'@'%' IDENTIFIED BY '"
              + MYSQL.getPassword()
              + "'");
      for (String table : OPTIMIZER_TABLES) {
        String privileges =
            table.equals("optimizer_schema_history")
                ? "SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX"
                : "SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, ALTER, DROP";
        statement.execute(
            "GRANT "
                + privileges
                + " ON `"
                + database
                + "`.`"
                + table
                + "` TO '"
                + SERVICE_USER
                + "'@'%'");
      }
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
        + "?useSSL=false&allowPublicKeyRetrieval=true&socketTimeout="
        + MySqlContainerInitializer.SOCKET_TIMEOUT_MILLIS;
  }
}
