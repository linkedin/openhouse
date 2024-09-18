package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.Pair;

/** A greedy candidate selector that selects the top K data layout strategies, ranked by scores. */
public class TopKDataLayoutCandidateSelector extends BaseDataLayoutCandidateSelector {

  private int k;

  public TopKDataLayoutCandidateSelector(int k) {
    this.k = k;
  }

  @Override
  protected List<Integer> filter(PriorityQueue<Pair<DataLayoutStrategy, Integer>> maxHeap) {
    List<Integer> result = new ArrayList<>();
    while (!maxHeap.isEmpty() && result.size() < this.k) {
      Pair<DataLayoutStrategy, Integer> dataLayoutStrategyIntegerPair = maxHeap.poll();
      result.add(dataLayoutStrategyIntegerPair.getRight());
    }
    return result;
  }
}
