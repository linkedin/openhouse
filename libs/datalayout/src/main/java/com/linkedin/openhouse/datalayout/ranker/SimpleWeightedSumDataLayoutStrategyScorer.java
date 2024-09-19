package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;

/**
 * Optimizing across multiple objective such as (x) Maximize file count reduction (x) Minimize
 * compute cost used for compaction by first normalizing the objectives on a common scale and then
 * performing scalarization using a weighted sum technique.
 */
public class SimpleWeightedSumDataLayoutStrategyScorer implements DataLayoutStrategyScorer {

  public static final int MINIMIZATION_OBJECTIVE_FACTOR = -1;

  private final double costWeight;
  private final double gainWeight;

  public SimpleWeightedSumDataLayoutStrategyScorer(double gainWeight, double costWeight) {
    this.gainWeight = gainWeight;
    this.costWeight = costWeight;
  }

  /**
   * Compute scores for the data layout strategies based on the input data.
   *
   * @param dataLayoutStrategies the data layout strategies to score
   * @return the data layout strategies w/ scores
   */
  @Override
  public List<ScoredDataLayoutStrategy> scoreDataLayoutStrategies(
      List<DataLayoutStrategy> dataLayoutStrategies) {

    List<ScoredDataLayoutStrategy> normalizedDataLayoutStrategies = new ArrayList<>();
    double minCost = minCost(dataLayoutStrategies);
    double maxCost = maxCost(dataLayoutStrategies);
    double minGain = minGain(dataLayoutStrategies);
    double maxGain = maxGain(dataLayoutStrategies);

    for (DataLayoutStrategy dataLayoutStrategy : dataLayoutStrategies) {
      double normalizedCost = minMaxNormalize(dataLayoutStrategy.getCost(), minCost, maxCost);
      double normalizedGain = minMaxNormalize(dataLayoutStrategy.getGain(), minGain, maxGain);
      ScoredDataLayoutStrategy normalizedDataLayoutStrategy =
          ScoredDataLayoutStrategy.builder()
              .dataLayoutStrategy(
                  DataLayoutStrategy.builder()
                      .config(dataLayoutStrategy.getConfig())
                      .entropy(dataLayoutStrategy.getEntropy())
                      .cost(dataLayoutStrategy.getCost())
                      .gain(dataLayoutStrategy.getGain())
                      .build())
              .normalizedComputeCost(normalizedCost)
              .normalizedFileCountReduction(normalizedGain)
              .score(
                  (gainWeight * normalizedGain)
                      + (MINIMIZATION_OBJECTIVE_FACTOR * costWeight * normalizedCost))
              .build();
      normalizedDataLayoutStrategies.add(normalizedDataLayoutStrategy);
    }

    return normalizedDataLayoutStrategies;
  }

  private double minMaxNormalize(double value, double min, double max) {
    if (max == min) {
      return 0.0;
    }
    return (value - min) / (max - min);
  }

  private double minCost(List<DataLayoutStrategy> dataLayoutStrategies) {
    return dataLayoutStrategies.stream().mapToDouble(DataLayoutStrategy::getCost).min().orElse(0);
  }

  private double maxCost(List<DataLayoutStrategy> dataLayoutStrategies) {
    return dataLayoutStrategies.stream().mapToDouble(DataLayoutStrategy::getCost).max().orElse(0);
  }

  private double minGain(List<DataLayoutStrategy> dataLayoutStrategies) {
    return dataLayoutStrategies.stream().mapToDouble(DataLayoutStrategy::getGain).min().orElse(0);
  }

  private double maxGain(List<DataLayoutStrategy> dataLayoutStrategies) {
    return dataLayoutStrategies.stream().mapToDouble(DataLayoutStrategy::getGain).max().orElse(0);
  }
}
