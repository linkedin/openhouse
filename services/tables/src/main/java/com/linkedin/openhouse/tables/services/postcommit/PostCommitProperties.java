package com.linkedin.openhouse.tables.services.postcommit;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

// Configuration for the Tables Service post-commit operation framework. Property prefix is
// tables.postcommit.
//
// When enabled is false (the default), the dispatcher bean is not constructed, and any
// registered PostCommitOperation beans are never invoked.
@ConfigurationProperties("tables.postcommit")
@Getter
@Setter
public class PostCommitProperties {

  // Master switch. When false, no operations are dispatched on commit.
  private boolean enabled = false;

  // Wall-clock ceiling that the dispatcher applies to each operation's prepared Mono.
  //
  // This bounds resource occupancy (connections, threads) for a misbehaving operation. It does
  // not block the commit thread, because operations run on the underlying reactive scheduler.
  //
  // Default is 3000 ms.
  private long perOpTimeoutMs = 3000L;
}
