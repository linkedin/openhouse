package com.linkedin.openhouse.tables.services.optimizer;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

// Configuration for the post-commit stats push from the Tables Service to the Optimizer's stats
// endpoint. Property prefix is optimizer.stats.
//
// When enabled is false (the default), no client bean is constructed, and the operation is
// absent from the dispatcher. Environments that want the feed turn it on and set baseUri.
@ConfigurationProperties("optimizer.stats")
@Getter
@Setter
public class OptimizerStatsProperties {

  // Master switch. When false, no HTTP push is attempted and no client bean is wired.
  private boolean enabled = false;

  // Base URI of the Optimizer service. The path /v1/optimizer/stats/{tableUuid} is appended at
  // call time. No default; required when enabled is true.
  private String baseUri;

  // Per-attempt HTTP timeout in milliseconds.
  //
  // Bounds each individual attempt so retries can fit inside the dispatcher's outer per-op budget
  // (tables.postcommit.per-op-timeout-ms, default 3000 ms). Default is 1000 ms.
  private long perAttemptTimeoutMs = 1000L;

  // Total attempt count (the initial try plus retries). Default is 3.
  //
  // Retries fire only on retryable errors: network failures, a timeout, or an HTTP 408 / 429 /
  // 5xx response. Other 4xx responses fail fast without retry.
  private int maxAttempts = 3;
}
