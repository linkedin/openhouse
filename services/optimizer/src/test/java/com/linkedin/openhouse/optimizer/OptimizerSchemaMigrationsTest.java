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
import java.util.Collections;
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
 * Starts the service on a database that already holds HouseTables' tables, as the database it
 * shares with HouseTables does, and checks the migrations against the JPA entities. The database
 * must end up with the tables that Flyway creates in an empty one, and keep HouseTables' table and
 * its rows; those tables must be the ones the entities define.
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
    startService(
        "new_database",
        // Hibernate also writes the DDL of the tables the entities define, before it validates the
        // entities. A script action turns ddl-auto off, so validation is a JPA action here too.
        "--spring.jpa.properties.javax.persistence.schema-generation.scripts.action=create",
        "--spring.jpa.properties.javax.persistence.schema-generation.scripts.create-target="
            + entitySchema(),
        "--spring.jpa.properties.hibernate.hbm2ddl.schema-generation.script.append=false",
        "--spring.jpa.properties.hibernate.hbm2ddl.delimiter=;",
        "--spring.jpa.properties.javax.persistence.schema-generation.database.action=validate");
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

  /**
   * Fails when an entity changes without a migration that makes the same change. When the service
   * started on the empty database, Hibernate validated the entities against the migrated tables and
   * wrote the DDL of the tables the entities define; this runs that DDL on another empty database
   * and compares the two, indexes included.
   */
  @Test
  void migrationsCreateTheTablesTheEntitiesDefine() throws SQLException {
    createDatabase("entities");
    try (Connection connection = connect("entities")) {
      ScriptUtils.executeSqlScript(connection, new FileSystemResource(entitySchema()));
    }
    Map<String, List<String>> migrated = new TreeMap<>(emptyDatabaseTables);
    migrated.remove("optimizer_schema_history");
    assertThat(migrated)
        .as(
            "Tables the migrations create. If an entity changed, generate its migration: see"
                + " docs/development/optimizer-schema-migrations.md")
        .isEqualTo(tables("entities"));
  }

  /**
   * Starts the service on the database, which migrates it and checks that the entities match the
   * result (ddl-auto=validate), and stops the service.
   */
  private static void startService(String database, String... args) {
    List<String> serviceArgs =
        new ArrayList<>(
            Arrays.asList(
                "--cluster.optimizer.database.url=" + jdbcUrl(database),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.jpa.hibernate.ddl-auto=validate"));
    serviceArgs.addAll(Arrays.asList(args));
    new SpringApplicationBuilder(OptimizerServiceApplication.class)
        .web(WebApplicationType.NONE)
        .run(serviceArgs.toArray(new String[0]))
        .close();
  }

  /** The file Hibernate writes the entities' DDL to. Gradle sets it; atlas.hcl reads it. */
  private static String entitySchema() {
    return Objects.requireNonNull(
        System.getProperty("optimizer.entity-schema"),
        "Run through Gradle, which sets optimizer.entity-schema: ./gradlew :services:optimizer:test");
  }

  /**
   * Each table's SHOW CREATE TABLE, a line per element, with the column and index lines sorted:
   * MySQL lists them in the order they were added, which differs between the migrations and
   * Hibernate's DDL.
   */
  private static Map<String, List<String>> tables(String database) throws SQLException {
    Map<String, List<String>> tables = new TreeMap<>();
    try (Connection connection = connect(database);
        Statement statement = connection.createStatement()) {
      for (String table : tableNames(statement)) {
        try (ResultSet result = statement.executeQuery("SHOW CREATE TABLE `" + table + "`")) {
          result.next();
          List<String> lines = new ArrayList<>(Arrays.asList(result.getString(2).split("\n")));
          List<String> elements = lines.subList(1, lines.size() - 1);
          elements.replaceAll(line -> line.trim().replaceAll(",$", ""));
          Collections.sort(elements);
          tables.put(table, lines);
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
