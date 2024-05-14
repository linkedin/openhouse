package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import lombok.Builder;

/**
 * Retrieves only a single ordering based on the respective column frequencies, namely the
 * one that has the columns ordered by most frequent accesses.
 */
@Builder
public class MostFrequentColumnOrderGenerator<ColumnStats>
    implements FrequencyColumnOrderGenerator {
    private final ColumnStats columnStats;

    @Override
    public List<List<String>> generateOrder(Set<String> availableColumns) {
        Map<Integer, String> frequencyOrdering = new TreeMap<Integer, String>(Collections.reverseOrder());
        for (Map.Entry<String, Integer> entry : columnStats.computeColumnFrequencies().entrySet()) {
            if (availableColumns.containsKey(entry.getKey())) {
                frequencyOrdering.put(entry.getKey(), entry.getValue());
            }
        }
        List<List<String> result = new ArrayList<String>();
        result.add(frequencyOrdering.values());

        return result;
    }
}
