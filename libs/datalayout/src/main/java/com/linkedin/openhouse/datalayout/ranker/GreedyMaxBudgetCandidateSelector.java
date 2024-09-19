package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A greedy candidate selector that selects the top K data layout strategies based on the max
 * budget. The max budget is defined by the max estimated compute cost or the max number of tables
 * whatever reaches first.
 */
public class GreedyMaxBudgetCandidateSelector extends BaseDataLayoutCandidateSelector {

  private final double maxEstimatedComputeCost;
  private final int maxTables;

  public GreedyMaxBudgetCandidateSelector(double maxEstimatedComputeCost, int maxTablesBudget) {
    this.maxEstimatedComputeCost = maxEstimatedComputeCost;
    this.maxTables = maxTablesBudget;
  }

  @Override
  protected List<Integer> filter(PriorityQueue<Pair<ScoredDataLayoutStrategy, Integer>> maxHeap) {
    List<Integer> result = new ArrayList<>();
    double totalEstimatedComputeCost = 0;
    int totalTables = 0;
    while (!maxHeap.isEmpty()
        && totalEstimatedComputeCost < this.maxEstimatedComputeCost
        && totalTables < this.maxTables) {
      Pair<ScoredDataLayoutStrategy, Integer> dataLayoutStrategyIntegerPair = maxHeap.poll();
      result.add(dataLayoutStrategyIntegerPair.getRight());
      totalEstimatedComputeCost +=
          dataLayoutStrategyIntegerPair.getLeft().getDataLayoutStrategy().getCost();
      totalTables += 1;
    }
    return result;
  }
}
