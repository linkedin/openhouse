package com.linkedin.openhouse.optimizer.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing over pre-projected {@link BinItem}s. Sibling of {@link
 * FirstFitBinPacker}: that one is operation-aware and projects {@code (op, stats)} internally,
 * while this one accepts already-projected items and is the right fit for callers without an
 * optimizer-service stats map (e.g. the legacy {@code JobsScheduler}).
 *
 * <p>Each bin has two independent caps:
 *
 * <ul>
 *   <li>{@code maxWeightPerBin} — total {@link BinItem#getWeight()}
 *   <li>{@code maxItemsPerBin} — number of items per bin
 * </ul>
 *
 * <p>An item whose weight exceeds the cap on its own goes into a bin by itself rather than being
 * dropped — we never silently skip maintenance work for an oversized table.
 *
 * <p>Pass {@code 0} or a negative value for either cap to disable that dimension.
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker {

  @Builder.Default private final long maxWeightPerBin = 1_000_000L;
  @Builder.Default private final int maxItemsPerBin = 50;

  /**
   * Pack {@code items} into bins tagged with {@code operationType}. Items are sorted by weight
   * descending, then each is placed into the first bin whose totals still fit.
   */
  public List<Bin> pack(List<BinItem> items, OperationTypeDto operationType) {
    if (items == null || items.isEmpty()) {
      return new ArrayList<>();
    }

    List<BinItem> sorted =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(Collectors.toList());

    List<PackingBin> bins = new ArrayList<>();
    for (BinItem item : sorted) {
      PackingBin target = null;
      for (PackingBin bin : bins) {
        if (bin.fits(item, maxWeightPerBin, maxItemsPerBin)) {
          target = bin;
          break;
        }
      }
      if (target == null) {
        target = new PackingBin();
        bins.add(target);
        if (!target.fits(item, maxWeightPerBin, maxItemsPerBin)) {
          log.warn(
              "Item exceeds per-bin caps on its own; placing in dedicated bin: fqtn={} weight={}",
              item.getFullyQualifiedTableName(),
              item.getWeight());
        }
      }
      target.add(item);
    }
    log.info("Packed {} items into {} bins", items.size(), bins.size());
    return bins.stream().map(pb -> new Bin(operationType, pb.items)).collect(Collectors.toList());
  }

  /** Running-totals helper used during the placement loop; collapsed into a {@link Bin} at end. */
  private static final class PackingBin {
    final List<BinItem> items = new ArrayList<>();
    long totalWeight;

    boolean fits(BinItem item, long maxWeight, int maxItems) {
      if (maxItems > 0 && items.size() >= maxItems) {
        return false;
      }
      if (maxWeight > 0 && totalWeight + item.getWeight() > maxWeight) {
        return false;
      }
      return true;
    }

    void add(BinItem item) {
      items.add(item);
      totalWeight += item.getWeight();
    }
  }
}
