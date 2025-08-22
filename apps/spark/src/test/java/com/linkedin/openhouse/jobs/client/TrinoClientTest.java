package com.linkedin.openhouse.jobs.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

public class TrinoClientTest {

  private static final String TEST_JDBC_URL = "jdbc:trino://test-host:8080";
  private static final String TEST_USERNAME = "test-user";
  private static final String TEST_PASSWORD = "test-password";
  private static final int TEST_QUERY_TIMEOUT = 300;
  private static final int TEST_FETCH_SIZE = 1000;

  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private ResultSetMetaData mockResultSetMetaData;

  private TrinoClient trinoClient;
  private RetryTemplate retryTemplate;

  @BeforeEach
  void setup() throws SQLException {
    // Initialize mocks
    MockitoAnnotations.openMocks(this);

    // Setup retry template with no retry policy for testing
    RetryPolicy retryPolicy = new NeverRetryPolicy();
    retryTemplate = RetryTemplate.builder().customPolicy(retryPolicy).build();

    // Setup mock connection behavior
    Mockito.when(mockConnection.prepareStatement(Mockito.anyString()))
        .thenReturn(mockPreparedStatement);

    // Setup mock statement behavior
    Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    // Setup mock result set behavior
    Mockito.when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    Mockito.when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    Mockito.when(mockResultSetMetaData.getColumnName(1)).thenReturn("column1");
    Mockito.when(mockResultSetMetaData.getColumnName(2)).thenReturn("column2");

    trinoClient =
        new TrinoClient(
            retryTemplate,
            TEST_JDBC_URL,
            TEST_USERNAME,
            TEST_PASSWORD,
            new Properties(),
            TEST_QUERY_TIMEOUT,
            TEST_FETCH_SIZE);
  }

  @Test
  void testExecuteQuerySuccess() throws SQLException {
    // Setup mock result set to return one row
    Mockito.when(mockResultSet.next()).thenReturn(true, false);
    Mockito.when(mockResultSet.getObject(1)).thenReturn("value1");
    Mockito.when(mockResultSet.getObject(2)).thenReturn("value2");

    // Mock DriverManager.getConnection to return our mock connection
    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(TEST_JDBC_URL, TEST_USERNAME, TEST_PASSWORD))
          .thenReturn(mockConnection);

      List<Map<String, Object>> results = trinoClient.executeQuery("SELECT * FROM test_table");

      Assertions.assertNotNull(results);
      Assertions.assertEquals(1, results.size());

      Map<String, Object> row = results.get(0);
      Assertions.assertEquals("value1", row.get("column1"));
      Assertions.assertEquals("value2", row.get("column2"));

      // Verify statement configuration
      Mockito.verify(mockPreparedStatement).setQueryTimeout(TEST_QUERY_TIMEOUT);
      Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }
  }

  @Test
  void testExecuteQueryEmptyResult() throws SQLException {
    // Setup mock result set to return no rows
    Mockito.when(mockResultSet.next()).thenReturn(false);

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(TEST_JDBC_URL, TEST_USERNAME, TEST_PASSWORD))
          .thenReturn(mockConnection);

      List<Map<String, Object>> results = trinoClient.executeQuery("SELECT * FROM empty_table");

      Assertions.assertNotNull(results);
      Assertions.assertEquals(0, results.size());
    }
  }

  @Test
  void testExecuteQueryMultipleRows() throws SQLException {
    // Setup mock result set to return multiple rows
    Mockito.when(mockResultSet.next()).thenReturn(true, true, true, false);
    Mockito.when(mockResultSet.getObject(1)).thenReturn("value1", "value2", "value3");
    Mockito.when(mockResultSet.getObject(2)).thenReturn("data1", "data2", "data3");

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(TEST_JDBC_URL, TEST_USERNAME, TEST_PASSWORD))
          .thenReturn(mockConnection);

      List<Map<String, Object>> results = trinoClient.executeQuery("SELECT * FROM test_table");

      Assertions.assertNotNull(results);
      Assertions.assertEquals(3, results.size());

      // Verify first row
      Map<String, Object> firstRow = results.get(0);
      Assertions.assertEquals("value1", firstRow.get("column1"));
      Assertions.assertEquals("data1", firstRow.get("column2"));

      // Verify second row
      Map<String, Object> secondRow = results.get(1);
      Assertions.assertEquals("value2", secondRow.get("column1"));
      Assertions.assertEquals("data2", secondRow.get("column2"));

      // Verify third row
      Map<String, Object> thirdRow = results.get(2);
      Assertions.assertEquals("value3", thirdRow.get("column1"));
      Assertions.assertEquals("data3", thirdRow.get("column2"));
    }
  }

  @Test
  void testExecuteQueryWithDifferentColumnCounts() throws SQLException {
    // Test with different column counts
    Mockito.when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    Mockito.when(mockResultSetMetaData.getColumnName(1)).thenReturn("single_column");
    Mockito.when(mockResultSet.next()).thenReturn(true, false);
    Mockito.when(mockResultSet.getObject(1)).thenReturn("single_value");

    try (MockedStatic<DriverManager> mockedDriverManager =
        Mockito.mockStatic(DriverManager.class)) {
      mockedDriverManager
          .when(() -> DriverManager.getConnection(TEST_JDBC_URL, TEST_USERNAME, TEST_PASSWORD))
          .thenReturn(mockConnection);

      List<Map<String, Object>> results =
          trinoClient.executeQuery("SELECT single_column FROM test_table");

      Assertions.assertNotNull(results);
      Assertions.assertEquals(1, results.size());

      Map<String, Object> row = results.get(0);
      Assertions.assertEquals("single_value", row.get("single_column"));
      Assertions.assertEquals(1, row.size()); // Only one column
    }
  }
}
