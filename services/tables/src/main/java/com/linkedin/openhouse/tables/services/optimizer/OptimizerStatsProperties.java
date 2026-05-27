package com.linkedin.openhouse.tables.services.optimizer;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the post-commit stats push from the Tables Service to the Optimizer's stats
 * endpoint. Property prefix: {@code optimizer.stats}.
 *
 * <p>When {@code enabled} is false (the default), no client bean is constructed and the push is a
 * no-op on every commit. Environments that want the feed turn it on and set {@link #baseUri}.
 */
@ConfigurationProperties("optimizer.stats")
@Getter
@Setter
public class OptimizerStatsProperties {

  /** Master switch. When {@code false}, no HTTP push is attempted and no client bean is wired. */
  private boolean enabled = false;

  /**
   * Base URI of the Optimizer service. Path {@code /v1/optimizer/stats/{tableUuid}} is appended at
   * call time. No default — required when {@link #enabled} is {@code true}.
   */
  private String baseUri;

  /**
   * Per-attempt HTTP timeout in milliseconds. Bounds each individual attempt so retries can fit
   * inside the dispatcher's outer per-op budget ({@code tables.postcommit.per-op-timeout-ms},
   * default 3000 ms). Default 1000 ms.
   */
  private long perAttemptTimeoutMs = 1000L;

  /**
   * Total attempt count (initial try plus retries). Default 3. Retries fire only on retryable
   * errors (network, timeout, 408/429/5xx); other 4xx fail fast without retry.
   */
  private int maxAttempts = 3;
}
