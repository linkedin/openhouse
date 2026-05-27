package com.linkedin.openhouse.tables.services.optimizer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

/**
 * Wiring for the post-commit Tables → Optimizer stats push. Only active when {@code
 * optimizer.stats.enabled=true}; otherwise no WebClient or client bean is constructed and the
 * on-commit hook in {@code IcebergSnapshotsServiceImpl} is a no-op.
 */
@Configuration
@EnableConfigurationProperties(OptimizerStatsProperties.class)
@ConditionalOnProperty(prefix = "optimizer.stats", name = "enabled", havingValue = "true")
public class OptimizerStatsConfig {

  /**
   * Dedicated WebClient for the optimizer stats endpoint. Per-attempt and outer timeouts are
   * applied at the call site on the Reactor chain in {@link OptimizerStatsClient} — they are not
   * configured on the underlying Netty client so that the timeout always emerges as a standard
   * {@link java.util.concurrent.TimeoutException} (not a Netty {@code ReadTimeoutException}), which
   * keeps the client's outcome classification simple.
   */
  @Bean("optimizerStatsWebClient")
  public WebClient optimizerStatsWebClient(OptimizerStatsProperties properties) {
    return WebClient.builder()
        .baseUrl(properties.getBaseUri())
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .build();
  }
}
