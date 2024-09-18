package com.linkedin.openhouse.datalayout.strategy;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for data layout optimization. Supports compaction and optionally sorting. The score
 * is used to rank the layout configurations.
 */
@Data
@Builder
public class DataLayoutStrategy {
  private final double score;
  private final double entropy;
  // TODO: refactor cost -> estimated_compute_cost, gain -> estimated_file_count_reduction
  private final double cost;
  private final double gain;
  private final DataCompactionConfig config;
  private final double normalizedComputeCost;
  private final double normalizedFileCountReduction;
  // TODO: support sorting config
}
