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
  private final double cost;
  private final double gain;
  private final DataCompactionConfig config;
  // TODO: support sorting config
}
