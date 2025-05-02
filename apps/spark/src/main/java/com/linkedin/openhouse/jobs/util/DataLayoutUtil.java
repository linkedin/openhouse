package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.ranker.DataLayoutCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DataLayoutUtil {
  private DataLayoutUtil() {
    // private method of static utility class
  }

  public static List<TableDataLayoutMetadata> selectStrategies(
      DataLayoutStrategyScorer scorer,
      DataLayoutCandidateSelector selector,
      List<TableDataLayoutMetadata> tableDataLayoutMetadataList) {
    // filter out non-primary tables
    tableDataLayoutMetadataList =
        tableDataLayoutMetadataList.stream()
            .filter(TableMetadata::isPrimary)
            .collect(Collectors.toList());
    log.info("Fetched metadata for {} data layout strategies", tableDataLayoutMetadataList.size());
    tableDataLayoutMetadataList =
        tableDataLayoutMetadataList.stream()
            // filter out strategies with no gain/file count reduction
            // or discounted to 0, e.g. frequently overwritten un-partitioned tables
            .filter(
                m ->
                    m.getDataLayoutStrategy().getGain()
                            * (1.0 - m.getDataLayoutStrategy().getFileCountReductionPenalty())
                        >= 1.0)
            .collect(Collectors.toList());
    log.info("Filtered metadata for {} data layout strategies", tableDataLayoutMetadataList.size());
    List<DataLayoutStrategy> strategies =
        tableDataLayoutMetadataList.stream()
            .map(TableDataLayoutMetadata::getDataLayoutStrategy)
            .collect(Collectors.toList());
    List<ScoredDataLayoutStrategy> scoredStrategies = scorer.scoreDataLayoutStrategies(strategies);
    List<Integer> selectedStrategyIndices = selector.select(scoredStrategies);
    return selectedStrategyIndices.stream()
        .map(tableDataLayoutMetadataList::get)
        .collect(Collectors.toList());
  }
}
