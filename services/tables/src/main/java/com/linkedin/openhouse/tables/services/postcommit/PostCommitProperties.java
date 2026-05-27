package com.linkedin.openhouse.tables.services.postcommit;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Tables Service post-commit operation framework. Property prefix: {@code
 * tables.postcommit}.
 *
 * <p>When {@code enabled} is {@code false} (the default), no dispatcher bean is constructed and
 * registered {@link PostCommitOperation} beans, if any, are never invoked. Per-OH-instance roll-out
 * flips a single flag.
 */
@ConfigurationProperties("tables.postcommit")
@Getter
@Setter
public class PostCommitProperties {

  /** Master switch. When {@code false}, no operations are dispatched on commit. */
  private boolean enabled = false;

  /**
   * Wall-clock ceiling applied by the dispatcher to each operation's {@link
   * PostCommitOperation#prepare(com.linkedin.openhouse.tables.model.TableDto)} {@code Mono}. Bounds
   * how long a misbehaving operation can keep resources (connections, threads) tied up. Does not
   * bound commit-thread latency — operations run on the executing reactive scheduler, not on the
   * commit thread. Default 3000 ms.
   */
  private long perOpTimeoutMs = 3000L;
}
