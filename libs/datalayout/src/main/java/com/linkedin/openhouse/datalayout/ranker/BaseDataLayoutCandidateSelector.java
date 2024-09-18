package com.linkedin.openhouse.datalayout.ranker;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for data layout candidate selector. It provides the basic functionality to provide
 * consistent ranking strategy across multiple candidate selection strategies.
 */
public abstract class BaseDataLayoutCandidateSelector implements DataLayoutCandidateSelector {

  /**
   * Rank the candidates to perform data layout optimizations based on the scores.
   *
   * @param dataLayoutStrategies all data layout strategies with scores computed.
   * @return index of the selected data layout strategies ordered by scores.
   */
  @Override
  public List<Integer> select(List<DataLayoutStrategy> dataLayoutStrategies) {
    PriorityQueue<Pair<DataLayoutStrategy, Integer>> maxHeap =
        new PriorityQueue<>(
            new Comparator<Pair<DataLayoutStrategy, Integer>>() {

              /**
               * Compares its two arguments for order. Returns a negative integer, zero, or a
               * positive integer as the first argument is less than, equal to, or greater than the
               * second.
               *
               * <p>
               *
               * @param o1 the first object to be compared.
               * @param o2 the second object to be compared.
               * @return a negative integer, zero, or a positive integer as the first argument is
               *     less than, equal to, or greater than the second.
               * @throws NullPointerException if an argument is null and this comparator does not
               *     permit null arguments
               * @throws ClassCastException if the arguments' types prevent them from being compared
               *     by this comparator.
               */
              @Override
              public int compare(
                  Pair<DataLayoutStrategy, Integer> o1, Pair<DataLayoutStrategy, Integer> o2) {
                return Double.compare(o2.getLeft().getScore(), o1.getLeft().getScore());
              }
            });

    for (int i = 0; i < dataLayoutStrategies.size(); i++) {
      maxHeap.add(Pair.of(dataLayoutStrategies.get(i), i));
    }

    return filter(maxHeap);
  }

  protected abstract List<Integer> filter(PriorityQueue<Pair<DataLayoutStrategy, Integer>> maxHeap);
}
