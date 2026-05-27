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

  /** Per-attempt request timeout in milliseconds. Default 1000. */
  private long perAttemptTimeoutMs = 1000L;

  /**
   * Hard ceiling on total wall-clock time for the entire call (including retries) in milliseconds.
   * Default 2000. When the outer timeout fires, the chain is cancelled and the error is swallowed.
   */
  private long totalTimeoutMs = 2000L;

  /**
   * Total attempt count (initial try plus retries). Default 3. With 1000 ms per attempt and a 2000
   * ms outer ceiling, only about two attempts realistically fit on a slow path; the third is
   * available if attempts return quickly.
   */
  private int maxAttempts = 3;
}
