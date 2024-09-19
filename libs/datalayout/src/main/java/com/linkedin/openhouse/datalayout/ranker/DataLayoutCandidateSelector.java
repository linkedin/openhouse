package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.List;

public interface DataLayoutCandidateSelector {

  /**
   * Pick the candidates to perform data layout optimizations based on the scores.
   *
   * @param dataLayoutStrategies all data layout strategies with scores computed.
   * @return index of the selected data layout strategies.
   */
  List<Integer> select(List<ScoredDataLayoutStrategy> dataLayoutStrategies);
}
