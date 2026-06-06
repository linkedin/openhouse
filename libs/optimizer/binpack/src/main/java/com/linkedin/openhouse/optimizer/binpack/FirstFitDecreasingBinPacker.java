package com.linkedin.openhouse.optimizer.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing. Sorts items by weight descending, then places each into the first
 * group whose totals stay at or below {@code maxWeightPerBin} and {@code maxItemsPerBin}. An item
 * whose weight exceeds the cap on its own goes into a group by itself.
 *
 * <p>Construct via the generated builder. Defaults are sized for the orphan-files-deletion case
 * (1M-file weight cap, 50 tables per bin) but every field is overridable. Omit {@code
 * binItemSupplier} when you are passing pre-projected {@link BinItem}s and don't need the
 * projection path.
 *
 * <p>Three callable shapes, all sharing the same placement core:
 *
 * <ol>
 *   <li><b>Projection path</b> ({@link #pack(List, Map)}, the {@link BinPacker} contract) — takes
 *       raw operations + stats, calls {@code binItemSupplier.get().fromOpAndStats(op, stats)} per
 *       row, returns the resulting groupings. Operations whose {@code tableUuid} has no entry in
 *       {@code statsByTableUuid} are dropped. Requires {@code binItemSupplier}.
 *   <li><b>Pre-projected raw groupings</b> ({@link #pack(List)}) — caller has already projected
 *       items; packer returns {@code List<List<BinItem>>}.
 *   <li><b>Pre-projected tagged bins</b> ({@link #pack(List, OperationTypeDto)}) — caller has
 *       already projected items; packer wraps each grouping in a {@link Bin} tagged with the
 *       supplied operation type. Use this when the bins flow directly to a scheduler that expects
 *       the operation type alongside the items.
 * </ol>
 *
 * <p>Stateless: {@code pack} is a pure function over its arguments.
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker<T extends BinItem> implements BinPacker {

  /** Required only for the {@link #pack(List, Map)} projection path; {@code null} otherwise. */
  private final Supplier<T> binItemSupplier;

  @Builder.Default private final long maxWeightPerBin = 1_000_000L;

  @Builder.Default private final int maxItemsPerBin = 50;

  @Override
  public List<List<BinItem>> pack(
      List<TableOperationDto> operations, Map<String, TableStatsDto> statsByTableUuid) {
    if (binItemSupplier == null) {
      throw new IllegalStateException(
          "FirstFitDecreasingBinPacker built without a binItemSupplier; use pack(List<BinItem>) or pack(List<BinItem>, OperationTypeDto) instead");
    }
    List<BinItem> items =
        operations.stream()
            .filter(op -> statsByTableUuid.containsKey(op.getTableUuid()))
            .map(
                op ->
                    binItemSupplier
                        .get()
                        .fromOpAndStats(op, statsByTableUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());
    List<List<BinItem>> groupings = packCore(items);
    log.info(
        "Packed {} operations ({} items after projection) into {} groupings",
        operations.size(),
        items.size(),
        groupings.size());
    return groupings;
  }

  /** Pre-projected items; returns raw groupings. */
  public List<List<BinItem>> pack(List<BinItem> items) {
    List<List<BinItem>> groupings = packCore(items);
    log.info(
        "Packed {} pre-projected items into {} groupings",
        items == null ? 0 : items.size(),
        groupings.size());
    return groupings;
  }

  /** Pre-projected items; returns {@link Bin}s each tagged with {@code operationType}. */
  public List<Bin> pack(List<BinItem> items, OperationTypeDto operationType) {
    return pack(items).stream()
        .map(group -> new Bin(operationType, group))
        .collect(Collectors.toList());
  }

  private List<List<BinItem>> packCore(List<BinItem> items) {
    if (items == null || items.isEmpty()) {
      return new ArrayList<>();
    }
    List<PackingBin> packingBins =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(ArrayList::new, this::placeItem, List::addAll);
    return packingBins.stream().map(pb -> pb.items).collect(Collectors.toList());
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
