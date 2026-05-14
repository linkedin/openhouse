package com.linkedin.openhouse.internal.catalog;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the post-write HDFS replication bump applied to newly-written {@code
 * metadata.json} files inside {@link OpenHouseInternalTableOperations#doCommit}.
 *
 * <p>The bump exists so the synchronous metadata write can ack at the cluster HDFS default
 * replication (typically lower, for write-latency reasons), then be asynchronously-to-the-client
 * raised to a higher durable replication factor.
 *
 * <p>Defaults are safe-by-default: the bump is <b>disabled</b> unless explicitly enabled per
 * deployment. This makes the rollout opt-in per fabric.
 *
 * <p>Configuration keys (under prefix {@code openhouse.internal.catalog.metadata.replication}):
 *
 * <ul>
 *   <li>{@code enabled} (boolean, default {@code false}) - master switch for the bump
 *   <li>{@code target} (short, default {@code 30}) - replication factor to apply
 *   <li>{@code timeoutMs} (long, default {@code 10000}) - strict per-call timeout for the {@code
 *       FileSystem.setReplication} RPC. On timeout the bump is treated as a degraded-durability
 *       event (WARN + commit succeeds), not a commit failure.
 * </ul>
 */
@Configuration
@ConfigurationProperties(prefix = "openhouse.internal.catalog.metadata.replication")
@Getter
@Setter
public class MetadataReplicationProperties {

  /** Master switch. The bump is opt-in per deployment. */
  private boolean enabled = false;

  /**
   * Target HDFS replication factor applied to a newly-written {@code metadata.json}. Stored as
   * {@code int} for Spring binding convenience; the underlying HDFS API takes {@code short}.
   */
  private int target = CatalogConstants.DEFAULT_METADATA_FILE_HDFS_REPLICATION;

  /**
   * Strict per-call timeout for the {@code FileSystem.setReplication} RPC. On timeout the work is
   * abandoned and the commit succeeds.
   */
  private long timeoutMs = 10_000L;
}
