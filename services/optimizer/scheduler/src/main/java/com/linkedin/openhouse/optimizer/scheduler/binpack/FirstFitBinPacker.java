package com.linkedin.openhouse.optimizer.scheduler.binpack;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing packing, abstract over the concrete {@link BinItem} subtype {@code T}. The
 * subclass tells the packer how to construct {@code T} from a {@code (operation, stats)} pair via
 * {@link #create}; the base class handles the bucketing: sort by weight descending, then place each
 * item into the first group whose totals stay at or below {@code maxWeightPerBin} and {@code
 * maxItemsPerBin}. An item whose weight exceeds the cap on its own goes into a group by itself.
 * Operations whose {@code tableUuid} has no entry in the stats map are dropped.
 *
 * <p>Stateless: the constructor takes only immutable cap configuration; {@link #pack} is a pure
 * function over its arguments. The packer is operation-agnostic — the scheduler wraps each grouping
 * into a {@link Bin} with the registered operation type.
 */
@Slf4j
public abstract class FirstFitBinPacker<T extends BinItem> implements BinPacker {

  private final long maxWeightPerBin;
  private final int maxItemsPerBin;

  protected FirstFitBinPacker(long maxWeightPerBin, int maxItemsPerBin) {
    this.maxWeightPerBin = maxWeightPerBin;
    this.maxItemsPerBin = maxItemsPerBin;
  }

  /**
   * Construct one {@code T} for a single operation. Called by {@link #pack} for every operation
   * whose stats are available; implementations encode the projection from {@code (op, stats)} to
   * the concrete {@link BinItem} subtype.
   */
  protected abstract T create(TableOperationDto operation, TableStatsDto stats);

  @Override
  public final List<List<BinItem>> pack(
      List<TableOperationDto> operations, Map<String, TableStatsDto> statsByTableUuid) {
    List<BinItem> items =
        operations.stream()
            .filter(op -> statsByTableUuid.containsKey(op.getTableUuid()))
            .map(op -> (BinItem) create(op, statsByTableUuid.get(op.getTableUuid())))
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
