package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.jobs.util.RetryUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * A generic client for querying Trino using JDBC. Supports connection management, query execution,
 * and result processing.
 */
@Slf4j
@AllArgsConstructor
public class TrinoClient {
  private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;
  private static final int DEFAULT_FETCH_SIZE = 1000;

  private final RetryTemplate retryTemplate;
  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final Properties connectionProperties;
  private final int queryTimeoutSeconds;
  private final int fetchSize;

  /** Create a new builder instance. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Execute a query and return the results as a list of maps. Each map represents a row with column
   * names as keys.
   */
  public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<List<Map<String, Object>>, Exception>)
            context -> {
              try (Connection connection = getConnection();
                  PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setQueryTimeout(queryTimeoutSeconds);
                statement.setFetchSize(fetchSize);

                try (ResultSet resultSet = statement.executeQuery()) {
                  return processResultSet(resultSet);
                }
              }
            },
        new ArrayList<>());
  }

  /** Get a database connection. */
  private Connection getConnection() throws SQLException {
    if (username != null && password != null) {
      return DriverManager.getConnection(jdbcUrl, username, password);
    } else {
      return DriverManager.getConnection(jdbcUrl, connectionProperties);
    }
  }

  /** Process a ResultSet and convert it to a list of maps. */
  private List<Map<String, Object>> processResultSet(ResultSet resultSet) throws SQLException {
    List<Map<String, Object>> results = new ArrayList<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();

    while (resultSet.next()) {
      Map<String, Object> row = new HashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = resultSet.getObject(i);
        row.put(columnName, value);
      }
      results.add(row);
    }

    return results;
  }

  /** Builder class for TrinoClient configuration. */
  public static class Builder {
    private String jdbcUrl;
    private String username;
    private String password;
    private Properties connectionProperties;
    private int queryTimeoutSeconds = DEFAULT_QUERY_TIMEOUT_SECONDS;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private RetryTemplate retryTemplate;

    public Builder jdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder connectionProperties(Properties connectionProperties) {
      this.connectionProperties = connectionProperties;
      return this;
    }

    public Builder queryTimeoutSeconds(int queryTimeoutSeconds) {
      this.queryTimeoutSeconds = queryTimeoutSeconds;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder retryTemplate(RetryTemplate retryTemplate) {
      this.retryTemplate = retryTemplate;
      return this;
    }

    public TrinoClient build() {
      if (jdbcUrl == null) {
        throw new IllegalArgumentException("JDBC URL is required");
      }
      if (connectionProperties == null) {
        connectionProperties = new Properties();
      }
      if (retryTemplate == null) {
        retryTemplate = RetryTemplate.builder().build();
      }
      return new TrinoClient(
          retryTemplate,
          jdbcUrl,
          username,
          password,
          connectionProperties,
          queryTimeoutSeconds,
          fetchSize);
    }
  }
}
