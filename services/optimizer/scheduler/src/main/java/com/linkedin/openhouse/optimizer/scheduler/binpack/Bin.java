package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

/**
 * Mutable accumulator used by a {@link BinPacker} while assembling a batch. Callers receiving a
 * packed list of {@code Bin}s treat them as read-only — {@link #items()} returns an unmodifiable
 * view and the running total is exposed only via the getter.
 *
 * @param <T> concrete {@link BinItem} implementation carried by this bin
 */
@ToString
public class Bin<T extends BinItem> {
  private final List<T> items = new ArrayList<>();
  @Getter private long totalWeight;

  /**
   * Returns true iff adding {@code item} keeps the bin at or below both caps. A cap of {@code <= 0}
   * disables that dimension.
   */
  boolean fits(T item, long maxWeight, int maxItems) {
    if (maxItems > 0 && items.size() >= maxItems) {
      return false;
    }
    if (maxWeight > 0 && totalWeight + item.getWeight() > maxWeight) {
      return false;
    }
    return true;
  }

  void add(T item) {
    items.add(item);
    totalWeight += item.getWeight();
  }

  public List<T> items() {
    return Collections.unmodifiableList(items);
  }

  public int size() {
    return items.size();
  }
}
