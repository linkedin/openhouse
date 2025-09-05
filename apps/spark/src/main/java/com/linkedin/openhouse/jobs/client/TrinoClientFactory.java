package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.jobs.util.RetryUtil;
import java.util.Properties;
import lombok.AllArgsConstructor;
import org.springframework.retry.support.RetryTemplate;

/** A factory class for {@link TrinoClient}. */
@AllArgsConstructor
public class TrinoClientFactory {
  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final Properties connectionProperties;
  private final int queryTimeoutSeconds;
  private final int fetchSize;

  /** Create a TrinoClient with default retry template. */
  public TrinoClient create() {
    return create(RetryUtil.getTrinoClientRetryTemplate());
  }

  /** Create a TrinoClient with custom retry template. */
  public TrinoClient create(RetryTemplate retryTemplate) {
    return TrinoClient.builder()
        .jdbcUrl(jdbcUrl)
        .username(username)
        .password(password)
        .connectionProperties(connectionProperties)
        .queryTimeoutSeconds(queryTimeoutSeconds)
        .fetchSize(fetchSize)
        .retryTemplate(retryTemplate)
        .build();
  }

  /** Builder class for TrinoClientFactory configuration. */
  public static class Builder {
    private String jdbcUrl;
    private String username;
    private String password;
    private Properties connectionProperties;
    private int queryTimeoutSeconds = 300;
    private int fetchSize = 1000;

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

    public TrinoClientFactory build() {
      if (jdbcUrl == null) {
        throw new IllegalArgumentException("JDBC URL is required");
      }
      if (connectionProperties == null) {
        connectionProperties = new Properties();
      }
      return new TrinoClientFactory(
          jdbcUrl, username, password, connectionProperties, queryTimeoutSeconds, fetchSize);
    }
  }

  /** Create a new builder instance. */
  public static Builder builder() {
    return new Builder();
  }
}
