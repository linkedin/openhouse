package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Brute force column order generator that takes as input all available columns and generates all
 * possible permutations. TODO: Add a version of this with a limited number of columns.
 */
public class BruteForceColumnOrderGenerator implements ColumnOrderGenerator {

  public List<List<String>> generateOrder(Set<String> availableColumns) {
    return generatePermutations(new ArrayList<>(availableColumns));
  }

  private List<List<String>> generatePermutations(List<String> original) {
    if (original.isEmpty()) {
      List<List<String>> result = new ArrayList<>();
      result.add(new ArrayList<>());
      return result;
    }
    String addedElement = original.remove(original.size() - 1);
    List<List<String>> result = new ArrayList<>();
    for (List<String> subCandidate : generatePermutations(original)) {
      for (int index = 0; index <= subCandidate.size(); index++) {
        List<String> candidate = new ArrayList<>(subCandidate);
        candidate.add(index, addedElement);
        result.add(candidate);
      }
    }
    return result;
  }
}
