package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.List;

/** Interface for scoring data layout strategies. */
public interface DataLayoutStrategyScorer {
  /**
   * Compute scores for the data layout strategies based on the input data.
   *
   * @param dataLayoutStrategies the data layout strategies to score
   * @return the data layout strategies w/ computed scores
   */
  List<ScoredDataLayoutStrategy> scoreDataLayoutStrategies(
      List<DataLayoutStrategy> dataLayoutStrategies);
}
