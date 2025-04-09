package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final double maxComputeCostPerTable;
  private final boolean isPartitionScope;

  public GreedyMaxBudgetCandidateSelector(
      double maxEstimatedComputeCost,
      int maxTablesBudget,
      double maxComputeCostPerTable,
      boolean isPartitionScope) {
    this.maxEstimatedComputeCost = maxEstimatedComputeCost;
    this.maxTables = maxTablesBudget;
    this.maxComputeCostPerTable = maxComputeCostPerTable;
    this.isPartitionScope = isPartitionScope;
  }

  @Override
  protected List<Integer> filter(PriorityQueue<Pair<ScoredDataLayoutStrategy, Integer>> maxHeap) {
    if (isPartitionScope) {
      return filterForPartitionScope(maxHeap);
    } else {
      return filterForTableScope(maxHeap);
    }
  }

  private List<Integer> filterForTableScope(
      PriorityQueue<Pair<ScoredDataLayoutStrategy, Integer>> maxHeap) {
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

  private List<Integer> filterForPartitionScope(
      PriorityQueue<Pair<ScoredDataLayoutStrategy, Integer>> maxHeap) {
    List<Integer> result = new ArrayList<>();
    Map<String, Double> tableCosts = new HashMap<>();
    double totalEstimatedComputeCost = 0;
    while (!maxHeap.isEmpty()
        && totalEstimatedComputeCost < this.maxEstimatedComputeCost
        && tableCosts.size() < this.maxTables) {
      Pair<ScoredDataLayoutStrategy, Integer> dataLayoutStrategyIntegerPair = maxHeap.poll();
      double cost = dataLayoutStrategyIntegerPair.getLeft().getDataLayoutStrategy().getCost();
      totalEstimatedComputeCost += cost;
      String tableName = dataLayoutStrategyIntegerPair.getLeft().getDataLayoutStrategy().getFqtn();
      double currentTableCost = tableCosts.getOrDefault(tableName, 0.0);
      if (currentTableCost + cost <= this.maxComputeCostPerTable) {
        tableCosts.put(tableName, currentTableCost + cost);
        result.add(dataLayoutStrategyIntegerPair.getRight());
      }
    }
    return result;
  }
}
