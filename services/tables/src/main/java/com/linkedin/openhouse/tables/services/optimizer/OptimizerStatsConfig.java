package com.linkedin.openhouse.tables.services.optimizer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

// Wiring for the post-commit Tables-to-Optimizer stats push.
//
// This configuration is active only when optimizer.stats.enabled=true. When the flag is false,
// no WebClient bean is constructed, and the dispatcher sees no optimizer-stats operation.
@Configuration
@EnableConfigurationProperties(OptimizerStatsProperties.class)
@ConditionalOnProperty(prefix = "optimizer.stats", name = "enabled", havingValue = "true")
public class OptimizerStatsConfig {

  // Dedicated WebClient for the optimizer stats endpoint.
  //
  // The per-attempt timeout is applied on the Reactor chain in OptimizerStatsPostCommitOperation,
  // and the outer per-op timeout is applied by PostCommitDispatcher. Neither timeout is
  // configured on the underlying Netty client.
  //
  // This arrangement ensures that any timeout always emerges as a standard
  // java.util.concurrent.TimeoutException rather than a Netty ReadTimeoutException, which keeps
  // the dispatcher's outcome classification simple.
  @Bean("optimizerStatsWebClient")
  public WebClient optimizerStatsWebClient(OptimizerStatsProperties properties) {
    return WebClient.builder()
        .baseUrl(properties.getBaseUri())
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .build();
  }
}
