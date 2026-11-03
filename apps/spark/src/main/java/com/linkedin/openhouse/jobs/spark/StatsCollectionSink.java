package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import java.util.List;

/**
 * Destination for the artifacts a stats-collection job produces for a table. Injecting a sink makes
 * the collection apps functionally complete and end-to-end testable (swap in a capturing sink)
 * without subclassing to override a logger: the app collects, the sink publishes.
 *
 * <p>The four artifacts are published independently so a sink can route each to its own
 * destination. Only {@link #publishStats} is required to carry a value; the commit-/partition-level
 * calls are made only when the collector produced non-empty results.
 *
 * <p>The OSS default is {@link LoggingStatsCollectionSink}; a deployment provides its own
 * implementation (e.g. a Kafka producer) to make the data durable.
 */
public interface StatsCollectionSink {

  void publishStats(String fqtn, IcebergTableStats icebergTableStats);

  void publishCommitEvents(String fqtn, List<CommitEventTable> commitEvents);

  void publishPartitionEvents(String fqtn, List<CommitEventTablePartitions> partitionEvents);

  void publishPartitionStats(String fqtn, List<CommitEventTablePartitionStats> partitionStats);
}
