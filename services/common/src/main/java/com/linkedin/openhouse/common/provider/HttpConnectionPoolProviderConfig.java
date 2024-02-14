package com.linkedin.openhouse.common.provider;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import reactor.netty.resources.ConnectionProvider;

/** Config for creating custom HTTP connection pool using netty reactor library */
@Slf4j
public final class HttpConnectionPoolProviderConfig {

  private HttpConnectionPoolProviderConfig() {}
  // The maximum number of connections per connection pool
  private static final int MAX_CONNECTION_POOL_SIZE = 500;
  // Max time to keep requests in pending queue before acquiring a connection
  private static final int MAX_PENDING_TIME_SECONDS = 50;
  // Max wait time for idle state before closing channel
  private static final int MAX_IDLE_TIME_SECONDS = 240;

  // Every 5 mins the connection pool is regularly checked for connections that are applicable for
  // removal
  private static final int EVICT_IN_BACKGROUND_TIME_SECONDS = 300;

  /**
   * Returns custom connection provider
   *
   * @return ConnectionProvider
   */
  public static ConnectionProvider getCustomConnectionProvider(String name) {
    log.info("Creating custom HTTP connection provider with name: {}", name);
    return ConnectionProvider.builder(name)
        .maxConnections(MAX_CONNECTION_POOL_SIZE)
        .maxIdleTime(Duration.ofSeconds(MAX_IDLE_TIME_SECONDS))
        .pendingAcquireTimeout(Duration.ofSeconds(MAX_PENDING_TIME_SECONDS))
        .evictInBackground(Duration.ofSeconds(EVICT_IN_BACKGROUND_TIME_SECONDS))
        .build();
  }
}
