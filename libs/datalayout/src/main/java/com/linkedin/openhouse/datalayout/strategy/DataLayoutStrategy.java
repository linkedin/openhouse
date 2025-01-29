package com.linkedin.openhouse.datalayout.strategy;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import lombok.Builder;
import lombok.Data;

/** Raw traits for table to feed into data layout optimization strategy generator. */
@Data
@Builder
public class DataLayoutStrategy {
  // TODO: remove score from here.
  private final double score;
  private final double entropy;
  // TODO: refactor cost -> estimated_compute_cost, gain -> estimated_file_count_reduction
  private final double cost;
  private final double gain;
  private final DataCompactionConfig config;
  // TODO: support sorting config
  private final String partitionId;
}
