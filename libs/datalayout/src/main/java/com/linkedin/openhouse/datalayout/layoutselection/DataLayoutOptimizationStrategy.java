package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Configuration for data layout optimization. Supports compaction and optionally sorting. The score
 * is used to rank the layout configurations.
 */
@Getter
@Builder
@EqualsAndHashCode
public class DataLayoutOptimizationStrategy {
  private final double score;
  private final DataCompactionConfig config;
  // TODO: support sorting config
}
