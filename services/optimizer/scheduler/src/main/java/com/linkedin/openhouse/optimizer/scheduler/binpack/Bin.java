package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

/**
 * Mutable accumulator used by a {@link BinPacker} while assembling a batch. Callers receiving a
 * packed list of {@code Bin}s treat them as read-only — {@link #items()} returns an unmodifiable
 * view, and the running totals are exposed only via getters.
 *
 * <p>Structurally identical to {@code jobs.util.binpack.Bin} introduced by PR&nbsp;#599; see the
 * note on {@link BinItem} for the swap-out plan.
 */
@ToString
public class Bin {
  private final List<BinItem> items = new ArrayList<>();
  @Getter private long totalWeight;
  @Getter private long totalSizeBytes;

  /**
   * Returns true iff adding {@code item} would keep this bin at or below all three caps. A cap of
   * {@code <= 0} disables that dimension.
   */
  boolean fits(BinItem item, long maxWeight, long maxSizeBytes, int maxItems) {
    if (maxItems > 0 && items.size() >= maxItems) {
      return false;
    }
    if (maxWeight > 0 && totalWeight + item.getWeight() > maxWeight) {
      return false;
    }
    if (maxSizeBytes > 0 && totalSizeBytes + item.getSizeBytes() > maxSizeBytes) {
      return false;
    }
    return true;
  }

  void add(BinItem item) {
    items.add(item);
    totalWeight += item.getWeight();
    totalSizeBytes += item.getSizeBytes();
  }

  public List<BinItem> items() {
    return Collections.unmodifiableList(items);
  }

  public int size() {
    return items.size();
  }
}
