package com.linkedin.openhouse.datalayout.layoutselection.scoring;

import com.linkedin.openhouse.datalayout.datasource.QueryStats;
import java.util.ArrayList;
import lombok.Builder;

/**
 * Calculates the cost of a solution based on per-query column access statistics. I.e.,
 * if all accessed columns are contained in the proposed ordering, the score for that
 * ordering increases. The score here represents the ratio of queries that are fully
 * affected by column ordering.
 */
@Builder
public class AllColumnsAccessedScore<T extends QueryStats>
    implements QueryStatsScoreCalculator<T> {
  private final QueryStats queryStats;

  @Override
  public List<Double> getScore(List<List<String>> columnOrdering) {
    List<Double> result = new ArrayList<Double>();
    long maxScore = queryStats.get(0).size();

    for (List<String> ordering : columnOrdering) {
      // Compute score for each ordering
      double score = 0.0;
      for (QueryStat queryStat : queryStats) {
        if (ordering.containsAll(queryStat.getAccessedColumns())) {
          score += 1;
        }
      }
      result.add(score/maxScore);
    }
    return result;
  }
}
