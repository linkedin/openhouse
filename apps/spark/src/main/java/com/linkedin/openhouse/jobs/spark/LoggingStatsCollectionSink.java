package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Default OSS {@link StatsCollectionSink} that logs each artifact as JSON. Mirrors the historical
 * behavior of the single-table {@code TableStatsCollectionSparkApp} publish methods so the apps are
 * runnable out of the box; a deployment swaps in a durable sink (e.g. Kafka).
 */
@Slf4j
public class LoggingStatsCollectionSink implements StatsCollectionSink {

  private final Gson gson = new Gson();

  @Override
  public void publishStats(String fqtn, IcebergTableStats icebergTableStats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(gson.toJson(icebergTableStats));
  }

  @Override
  public void publishCommitEvents(String fqtn, List<CommitEventTable> commitEvents) {
    log.info("Publishing commit events for table: {}", fqtn);
    log.info(gson.toJson(commitEvents));
  }

  @Override
  public void publishPartitionEvents(
      String fqtn, List<CommitEventTablePartitions> partitionEvents) {
    log.info("Publishing partition events for table: {}", fqtn);
    log.info(gson.toJson(partitionEvents));
  }

  @Override
  public void publishPartitionStats(
      String fqtn, List<CommitEventTablePartitionStats> partitionStats) {
    log.info("Publishing partition stats for table: {} ({} stats)", fqtn, partitionStats.size());
    log.info(gson.toJson(partitionStats));
  }
}
