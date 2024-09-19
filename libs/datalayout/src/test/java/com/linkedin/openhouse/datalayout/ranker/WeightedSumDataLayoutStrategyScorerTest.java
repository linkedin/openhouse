package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WeightedSumDataLayoutStrategyScorerTest {

  private List<DataLayoutStrategy> testSampleDataLayoutStrategies;

  @BeforeEach
  void init() {
    DataLayoutStrategy lowScoreStrategy = DataLayoutStrategy.builder().cost(0.5).gain(1.0).build();
    DataLayoutStrategy midScoreStrategy =
        DataLayoutStrategy.builder().cost(50.0).gain(100.0).build();
    DataLayoutStrategy highScoreStrategy =
        DataLayoutStrategy.builder().cost(500.0).gain(1000.0).build();
    testSampleDataLayoutStrategies = new ArrayList<DataLayoutStrategy>();
    testSampleDataLayoutStrategies.add(lowScoreStrategy);
    testSampleDataLayoutStrategies.add(midScoreStrategy);
    testSampleDataLayoutStrategies.add(highScoreStrategy);
  }

  @Test
  public void testWeightedSumDataLayoutStrategyScorer() {
    DataLayoutStrategyScorer dataLayoutStrategyScorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(0.7, 0.3);
    List<ScoredDataLayoutStrategy> normalizedStrategies =
        dataLayoutStrategyScorer.scoreDataLayoutStrategies(testSampleDataLayoutStrategies);
    Assertions.assertEquals(normalizedStrategies.size(), 3);

    Assertions.assertEquals(normalizedStrategies.get(0).getNormalizedComputeCost(), 0);
    Assertions.assertEquals(normalizedStrategies.get(0).getNormalizedFileCountReduction(), 0);
    Assertions.assertEquals(normalizedStrategies.get(0).getScore(), 0);

    Assertions.assertEquals(
        normalizedStrategies.get(1).getNormalizedComputeCost(), (50.0 - 0.5) / (500.0 - 0.5));
    Assertions.assertEquals(
        normalizedStrategies.get(1).getNormalizedFileCountReduction(),
        (100 - 1.0) / (1000.0 - 1.0));
    Assertions.assertEquals(
        normalizedStrategies.get(1).getScore(),
        (0.7 * ((100 - 1.0) / (1000.0 - 1.0))) - (0.3 * ((50.0 - 0.5) / (500.0 - 0.5))));

    Assertions.assertEquals(normalizedStrategies.get(2).getNormalizedComputeCost(), 1.0);
    Assertions.assertEquals(normalizedStrategies.get(2).getNormalizedFileCountReduction(), 1.0);
    Assertions.assertEquals(normalizedStrategies.get(2).getScore(), (0.7 * 1.0) - (0.3 * 1.0));
  }

  @Test
  public void testWeightSumScorerTopKCandidateSelector() {
    DataLayoutStrategyScorer dataLayoutStrategyScorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(0.7, 0.3);
    List<ScoredDataLayoutStrategy> normalizedStrategies =
        dataLayoutStrategyScorer.scoreDataLayoutStrategies(testSampleDataLayoutStrategies);
    Assertions.assertEquals(normalizedStrategies.size(), 3);
    for (int k = 1; k <= 3; k++) {
      DataLayoutCandidateSelector dataLayoutCandidateSelector =
          new GreedyMaxBudgetCandidateSelector(Double.MAX_VALUE, k);
      List<Integer> topK = dataLayoutCandidateSelector.select(normalizedStrategies);
      Assertions.assertEquals(k, topK.size());
      for (int j = 1; j <= k; j++) {
        Assertions.assertEquals(3 - j, topK.get(j - 1));
      }
    }
  }

  @Test
  public void testWeightSumScorerMaxBudgetCandidateSelector() {
    DataLayoutStrategyScorer dataLayoutStrategyScorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(0.7, 0.3);
    List<ScoredDataLayoutStrategy> normalizedStrategies =
        dataLayoutStrategyScorer.scoreDataLayoutStrategies(testSampleDataLayoutStrategies);
    Assertions.assertEquals(3, normalizedStrategies.size());

    DataLayoutCandidateSelector selectAll = new GreedyMaxBudgetCandidateSelector(550.5, 3);
    List<Integer> selectedAllStrategies = selectAll.select(normalizedStrategies);
    Assertions.assertEquals(3, selectedAllStrategies.size());
    Assertions.assertArrayEquals(new Integer[] {2, 1, 0}, selectedAllStrategies.toArray());

    DataLayoutCandidateSelector selectTwo = new GreedyMaxBudgetCandidateSelector(550.0, 3);
    List<Integer> selectedTwoStrategies = selectTwo.select(normalizedStrategies);
    Assertions.assertEquals(2, selectedTwoStrategies.size());
    Assertions.assertArrayEquals(new Integer[] {2, 1}, selectedTwoStrategies.toArray());

    DataLayoutCandidateSelector selectOne = new GreedyMaxBudgetCandidateSelector(500.0, 3);
    List<Integer> selectedOneStrategies = selectOne.select(normalizedStrategies);
    Assertions.assertEquals(1, selectedOneStrategies.size());
    Assertions.assertArrayEquals(new Integer[] {2}, selectedOneStrategies.toArray());

    DataLayoutCandidateSelector selectNone = new GreedyMaxBudgetCandidateSelector(5000.0, 0);
    List<Integer> selectedNoneStrategies = selectNone.select(normalizedStrategies);
    Assertions.assertEquals(0, selectedNoneStrategies.size());
    Assertions.assertArrayEquals(new Integer[] {}, selectedNoneStrategies.toArray());
  }
}
