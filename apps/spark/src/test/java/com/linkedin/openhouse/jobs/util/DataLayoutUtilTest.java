package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.ranker.DataLayoutCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.ranker.GreedyMaxBudgetCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.SimpleWeightedSumDataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataLayoutUtilTest {
  private static final double GAIN_WEIGHT = 0.7;
  private static final double COST_WEIGHT = 0.3;

  @Test
  void testSelectStrategies() {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        Arrays.asList(
            TableDataLayoutMetadata.builder()
                .dataLayoutStrategy(
                    // large gain, but all discounted to 0 due to penalty, should be filtered out
                    DataLayoutStrategy.builder()
                        .gain(10000)
                        .cost(10)
                        .fileCountReductionPenalty(1.0)
                        .build())
                .dbName("db")
                .tableName("table1")
                .isPrimary(true)
                .build(),
            TableDataLayoutMetadata.builder()
                .dataLayoutStrategy(
                    DataLayoutStrategy.builder()
                        .gain(1000)
                        .cost(100)
                        .fileCountReductionPenalty(0.0)
                        .build())
                .dbName("db")
                .tableName("table2")
                // not primary, should be filtered out
                .isPrimary(false)
                .build(),
            TableDataLayoutMetadata.builder()
                .dataLayoutStrategy(
                    // small gain, but not discounted to 0, should be selected
                    DataLayoutStrategy.builder()
                        .gain(10)
                        .cost(10)
                        .fileCountReductionPenalty(0.0)
                        .build())
                .dbName("db")
                .tableName("table3")
                .isPrimary(true)
                .build(),
            TableDataLayoutMetadata.builder()
                .dataLayoutStrategy(
                    // medium gain and cost, should be selected
                    DataLayoutStrategy.builder()
                        .gain(1000)
                        .cost(100)
                        .fileCountReductionPenalty(0.0)
                        .build())
                .dbName("db")
                .tableName("table4")
                .isPrimary(true)
                .build());
    DataLayoutStrategyScorer scorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(GAIN_WEIGHT, COST_WEIGHT);
    double maxComputeCost = 1e6; // infinite
    int maxStrategiesCount = 1000; // infinite
    DataLayoutCandidateSelector candidateSelector =
        new GreedyMaxBudgetCandidateSelector(maxComputeCost, maxStrategiesCount);
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        DataLayoutUtil.selectStrategies(scorer, candidateSelector, tableDataLayoutMetadataList);
    Assertions.assertEquals(2, selectedTableDataLayoutMetadataList.size());
    List<String> expectedTableNames = Arrays.asList("table3", "table4");
    for (TableDataLayoutMetadata tableDataLayoutMetadata : selectedTableDataLayoutMetadataList) {
      Assertions.assertTrue(expectedTableNames.contains(tableDataLayoutMetadata.getTableName()));
    }
  }
}
