package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing algorithm. Sorts items by weight descending and places each into the
 * first group whose running totals stay at or below {@code maxWeightPerBin} and {@code
 * maxItemsPerBin}. An item that exceeds the weight cap on its own goes into a group by itself.
 *
 * <p>Returns flat groupings ({@code List<List<BinItem>>}). Callers wrap each grouping into the
 * {@link Bin} implementation they need for their operation type.
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker {

  private final long maxWeightPerBin;
  private final int maxItemsPerBin;

  public List<List<BinItem>> pack(List<BinItem> items) {
    if (items == null || items.isEmpty()) {
      return new ArrayList<>();
    }
    List<PackingBin> bins =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(ArrayList::new, this::placeItem, List::addAll);
    log.info("Packed {} items into {} groupings", items.size(), bins.size());
    return bins.stream().map(b -> b.items).collect(java.util.stream.Collectors.toList());
  }

  private void placeItem(List<PackingBin> bins, BinItem item) {
    bins.stream()
        .filter(b -> b.fits(item, maxWeightPerBin, maxItemsPerBin))
        .findFirst()
        .ifPresentOrElse(
            b -> b.add(item),
            () -> {
              PackingBin fresh = new PackingBin();
              fresh.add(item);
              bins.add(fresh);
            });
  }

  /** Per-bin running-totals helper used during the fold. Hidden from callers. */
  private static class PackingBin {
    final List<BinItem> items = new ArrayList<>();
    long totalWeight;

    boolean fits(BinItem item, long maxWeight, int maxItems) {
      return items.size() < maxItems && totalWeight + item.getWeight() <= maxWeight;
    }

    void add(BinItem item) {
      items.add(item);
      totalWeight += item.getWeight();
    }
  }
}
