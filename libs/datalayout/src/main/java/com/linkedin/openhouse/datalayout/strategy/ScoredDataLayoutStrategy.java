package com.linkedin.openhouse.datalayout.strategy;

import lombok.Builder;
import lombok.Data;

/** Data layout strategy with a score, and normalized units for traits. */
@Data
@Builder
public class ScoredDataLayoutStrategy {
  private final DataLayoutStrategy dataLayoutStrategy;
  private final double score;
  private final double normalizedComputeCost;
  private final double normalizedFileCountReduction;
}
