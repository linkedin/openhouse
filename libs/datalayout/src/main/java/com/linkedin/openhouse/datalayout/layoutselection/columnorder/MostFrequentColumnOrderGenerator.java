package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import com.linkedin.openhouse.datalayout.datasource.ColumnStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.Builder;

/**
 * Retrieves only a single ordering based on the respective column frequencies, namely the one that
 * has the columns ordered by most frequent accesses.
 */
@Builder
public class MostFrequentColumnOrderGenerator<T extends ColumnStats>
    implements FrequencyColumnOrderGenerator<T> {
  private final ColumnStats columnStats;

  @Override
  public List<List<String>> generateOrder(Set<String> availableColumns) {
    Map<Long, String> frequencyOrdering = new TreeMap<>(Collections.reverseOrder());
    columnStats
        .get()
        .collectAsList()
        .forEach(
            columnStat -> {
              if (availableColumns.contains(columnStat.getName())) {
                frequencyOrdering.put(columnStat.getAccessCount(), columnStat.getName());
              }
            });
    return Collections.singletonList(new ArrayList<>(frequencyOrdering.values()));
  }
}
