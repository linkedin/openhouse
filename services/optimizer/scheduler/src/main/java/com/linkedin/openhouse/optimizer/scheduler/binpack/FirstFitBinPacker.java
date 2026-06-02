package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing, generic over the concrete {@link BinItem} subtype {@code T}.
 * Construction takes a {@code Supplier<T>} — typically a {@code MyItem::new} method reference —
 * which the packer invokes per operation to get a seat, then calls {@link BinItem#fromOpAndStats}
 * on the seat to project the (operation, stats) pair into a populated item.
 *
 * <p>Sorts items by weight descending, then places each into the first group whose totals stay at
 * or below {@code maxWeightPerBin} and {@code maxItemsPerBin}. An item whose weight exceeds the cap
 * on its own goes into a group by itself. Operations whose {@code tableUuid} has no entry in {@code
 * statsByTableUuid} are dropped.
 *
 * <p>Stateless: the constructor takes only the seat factory and the cap configuration; {@link
 * #pack} is a pure function over its arguments. The packer is operation-agnostic — the scheduler
 * wraps each grouping into a {@link Bin} with the registered operation type.
 */
@Slf4j
public class FirstFitBinPacker<T extends BinItem> implements BinPacker {

  private final Supplier<T> seatFactory;
  private final long maxWeightPerBin;
  private final int maxItemsPerBin;

  public FirstFitBinPacker(Supplier<T> seatFactory, long maxWeightPerBin, int maxItemsPerBin) {
    this.seatFactory = seatFactory;
    this.maxWeightPerBin = maxWeightPerBin;
    this.maxItemsPerBin = maxItemsPerBin;
  }

  @Override
  public List<List<BinItem>> pack(
      List<TableOperationDto> operations, Map<String, TableStatsDto> statsByTableUuid) {
    List<BinItem> items =
        operations.stream()
            .filter(op -> statsByTableUuid.containsKey(op.getTableUuid()))
            .map(
                op -> seatFactory.get().fromOpAndStats(op, statsByTableUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());
    List<PackingBin> packingBins =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(ArrayList::new, this::placeItem, List::addAll);
    log.info(
        "Packed {} operations ({} items after projection) into {} groupings",
        operations.size(),
        items.size(),
        packingBins.size());
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
