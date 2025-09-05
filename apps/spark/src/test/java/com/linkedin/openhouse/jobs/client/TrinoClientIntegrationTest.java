package com.linkedin.openhouse.jobs.client;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Integration test for TrinoClient using H2 database directly. This test verifies the actual
 * functionality of TrinoClient against a real database.
 */
public class TrinoClientIntegrationTest {

  public static final String H2_USER = "sa";
  public static final String H2_PASSWORD = "";
  private static final String TEST_SCHEMA = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String H2_URL =
      "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
  private static TrinoClient trinoClient;
  private static Connection h2Connection;

  @BeforeAll
  static void setUp() throws Exception {
    // Start H2 database
    startH2Database();

    // Create TrinoClient (using H2 directly for testing)
    createTrinoClient();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (h2Connection != null && !h2Connection.isClosed()) {
      h2Connection.close();
    }
  }

  private static void startH2Database() throws SQLException {
    // Create H2 database connection
    h2Connection = DriverManager.getConnection(H2_URL, H2_USER, H2_PASSWORD);

    // Create test schema and table
    try (Statement stmt = h2Connection.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);

      // Create test table in the test schema
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS "
              + TEST_SCHEMA
              + "."
              + TEST_TABLE
              + " ("
              + "id INT PRIMARY KEY, "
              + "name VARCHAR(100), "
              + "age INT, "
              + "active BOOLEAN, "
              + "salary DECIMAL(10,2)"
              + ")");

      // Insert test data
      stmt.execute(
          "INSERT INTO "
              + TEST_SCHEMA
              + "."
              + TEST_TABLE
              + " VALUES "
              + "(1, 'John Doe', 30, true, 50000.0), "
              + "(2, 'Jane Smith', 25, false, 45000.0), "
              + "(3, 'Bob Johnson', 35, true, 60000.0), "
              + "(4, 'Alice Brown', 28, true, 52000.0), "
              + "(5, 'Charlie Wilson', 32, false, 48000.0)");
    }
  }

  private static void createTrinoClient() {
    NeverRetryPolicy neverRetryPolicy = new NeverRetryPolicy();
    RetryTemplate retryTemplate = RetryTemplate.builder().customPolicy(neverRetryPolicy).build();
    trinoClient =
        new TrinoClient(retryTemplate, H2_URL, H2_USER, H2_PASSWORD, new Properties(), 30, 1000);
  }

  @Test
  void testBasicQueryExecution() throws SQLException {
    // Execute a simple SELECT query
    List<Map<String, Object>> results =
        trinoClient.executeQuery(
            "SELECT * FROM " + TEST_SCHEMA + "." + TEST_TABLE + " ORDER BY id");

    assertNotNull(results);
    assertEquals(5, results.size());

    // Verify first row
    Map<String, Object> firstRow = results.get(0);
    assertEquals(1, firstRow.get("id".toUpperCase()));
    assertEquals("John Doe", firstRow.get("name".toUpperCase()));
    assertEquals(30, firstRow.get("age".toUpperCase()));
    assertEquals(true, firstRow.get("active".toUpperCase()));
    assertEquals(new BigDecimal("50000.00"), firstRow.get("salary".toUpperCase()));
  }

  @Test
  void testQueryWithWhereClause() throws SQLException {
    // Execute query with WHERE clause
    List<Map<String, Object>> results =
        trinoClient.executeQuery(
            "SELECT * FROM "
                + TEST_SCHEMA
                + "."
                + TEST_TABLE
                + " WHERE active = true AND age > 25");

    assertNotNull(results);
    assertEquals(3, results.size());

    // Verify all results are active and age > 25
    for (Map<String, Object> row : results) {
      assertTrue((Boolean) row.get("active".toUpperCase()));
      assertTrue((Integer) row.get("age".toUpperCase()) > 25);
    }
  }

  @Test
  void testQueryWithAggregation() throws SQLException {
    // Execute query with aggregation
    List<Map<String, Object>> results =
        trinoClient.executeQuery(
            "SELECT active, COUNT(*) as count, AVG(age) as avg_age, SUM(salary) as total_salary "
                + "FROM "
                + TEST_SCHEMA
                + "."
                + TEST_TABLE
                + " GROUP BY active");

    assertNotNull(results);
    assertEquals(2, results.size());

    // Find active and inactive groups
    Map<String, Object> activeGroup = null;
    Map<String, Object> inactiveGroup = null;

    for (Map<String, Object> row : results) {
      if ((Boolean) row.get("active".toUpperCase())) {
        activeGroup = row;
      } else {
        inactiveGroup = row;
      }
    }

    // Verify active group
    assertNotNull(activeGroup);
    assertEquals(3L, activeGroup.get("count".toUpperCase()));
    assertEquals(31.0, (Double) activeGroup.get("avg_age".toUpperCase()), 0.01);
    assertEquals(new BigDecimal("162000.00"), activeGroup.get("total_salary".toUpperCase()));

    // Verify inactive group
    assertNotNull(inactiveGroup);
    assertEquals(2L, inactiveGroup.get("count".toUpperCase()));
    assertEquals(28.5, (Double) inactiveGroup.get("avg_age".toUpperCase()), 0.01);
    assertEquals(new BigDecimal("93000.00"), inactiveGroup.get("total_salary".toUpperCase()));
  }

  @Test
  void testQueryWithOrderByAndLimit() throws SQLException {
    // Execute query with ORDER BY and LIMIT
    List<Map<String, Object>> results =
        trinoClient.executeQuery(
            "SELECT name, salary FROM "
                + TEST_SCHEMA
                + "."
                + TEST_TABLE
                + " ORDER BY salary DESC LIMIT 3");

    assertNotNull(results);
    assertEquals(3, results.size());

    // Verify results are ordered by salary descending
    assertEquals("Bob Johnson", results.get(0).get("name".toUpperCase()));
    assertEquals(new BigDecimal("60000.00"), results.get(0).get("salary".toUpperCase()));

    assertEquals("Alice Brown", results.get(1).get("name".toUpperCase()));
    assertEquals(new BigDecimal("52000.00"), results.get(1).get("salary".toUpperCase()));

    assertEquals("John Doe", results.get(2).get("name".toUpperCase()));
    assertEquals(new BigDecimal("50000.00"), results.get(2).get("salary".toUpperCase()));
  }

  @Test
  void testQueryWithDifferentDataTypes() throws SQLException {
    // Test query with different data types
    List<Map<String, Object>> results =
        trinoClient.executeQuery(
            "SELECT "
                + "CAST(id AS BIGINT) as bigint_id, "
                + "CAST(name AS VARCHAR) as varchar_name, "
                + "CAST(age AS INTEGER) as int_age, "
                + "CAST(active AS BOOLEAN) as bool_active, "
                + "CAST(salary AS DOUBLE) as double_salary "
                + "FROM "
                + TEST_SCHEMA
                + "."
                + TEST_TABLE
                + " WHERE id = 1");

    assertNotNull(results);
    assertEquals(1, results.size());

    Map<String, Object> row = results.get(0);
    assertEquals(1L, row.get("bigint_id".toUpperCase()));
    assertEquals("John Doe", row.get("varchar_name".toUpperCase()));
    assertEquals(30, row.get("int_age".toUpperCase()));
    assertEquals(true, row.get("bool_active".toUpperCase()));
    assertEquals(50000.0, row.get("double_salary".toUpperCase()));
  }
}
