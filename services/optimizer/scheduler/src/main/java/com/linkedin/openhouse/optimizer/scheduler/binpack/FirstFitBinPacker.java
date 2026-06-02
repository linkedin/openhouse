package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing. Sorts items by weight descending, then places each into the first
 * group whose totals stay at or below {@code maxWeightPerBin} and {@code maxItemsPerBin}. An item
 * whose weight exceeds the cap on its own goes into a group by itself.
 *
 * <p>Stateless: the constructor takes only immutable configuration; {@link #pack(List)} is a pure
 * function over its argument.
 */
@Slf4j
@AllArgsConstructor
public class FirstFitBinPacker implements BinPacker {

  @Getter private final OperationTypeDto operationType;
  private final long maxWeightPerBin;
  private final int maxItemsPerBin;

  @Override
  public List<Bin> pack(List<BinItem> items) {
    List<PackingBin> packingBins =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(ArrayList::new, this::placeItem, List::addAll);
    log.info("Packed {} items into {} bins", items.size(), packingBins.size());
    return packingBins.stream()
        .map(pb -> new Bin(operationType, pb.items))
        .collect(Collectors.toList());
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

  /** Running-totals helper used during the fold. */
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
