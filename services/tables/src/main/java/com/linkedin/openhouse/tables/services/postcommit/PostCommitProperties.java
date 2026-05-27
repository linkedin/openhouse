package com.linkedin.openhouse.tables.services.postcommit;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Tables Service post-commit operation framework. Property prefix {@code
 * tables.postcommit}. When {@code enabled} is false (the default), no dispatcher bean is
 * constructed and registered {@link PostCommitOperation} beans are never invoked.
 */
@ConfigurationProperties("tables.postcommit")
@Getter
@Setter
public class PostCommitProperties {

  /** Master switch. When false, no operations are dispatched on commit. */
  private boolean enabled = false;

  /**
   * Wall-clock ceiling applied by the dispatcher to each operation's prepared {@code Mono}. Bounds
   * resource occupancy (connections, threads) but does not block the commit thread. Default 3000
   * ms.
   */
  private long perOpTimeoutMs = 3000L;
}
