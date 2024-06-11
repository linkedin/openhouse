package com.linkedin.openhouse.datalayout.layoutselection.scoring;

import java.util.List;

/**
 * Score calculator interface to evaluate orderings of columns.
 * A higher score is better, the length of the input list matches the length
 * of the output list, and order should be maintained so that the score at
 * index i is the computed score for columnOrdering element i.
 */
public interface ScoreCalculator {
  List<Double> getScore(List<List<String>> columnOrdering);
}
