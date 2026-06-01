package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic first-fit-decreasing bin packer with two independent caps:
 *
 * <ul>
 *   <li>{@code maxWeightPerBin} — total {@link BinItem#getWeight()} per bin
 *   <li>{@code maxItemsPerBin} — number of items per bin
 * </ul>
 *
 * <p>Pass {@code 0} or a negative value for any cap to disable that dimension.
 *
 * <p>An item that exceeds the weight cap on its own is placed into a bin by itself rather than
 * dropped — the scheduler never silently skips maintenance work for an oversized table.
 *
 * <p>The pack body is a single stream pipeline: sort decreasing by weight, then fold each item into
 * the running list of bins. The fold uses {@code Stream.collect(Supplier, BiConsumer, BiConsumer)}
 * — the standard idiom for an FFD-style stateful collect — so the placement is expressed once, in
 * functional form, with the compiler enforcing {@code T}-consistency across the pipeline.
 *
 * @param <T> concrete {@link BinItem} implementation packed by this packer
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker<T extends BinItem> implements BinPacker<T> {

  @Builder.Default private final long maxWeightPerBin = 1_000_000L;
  @Builder.Default private final int maxItemsPerBin = 50;

  @Override
  public List<Bin<T>> pack(List<T> items) {
    if (items == null || items.isEmpty()) {
      return new ArrayList<>();
    }
    List<Bin<T>> bins =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(ArrayList::new, this::placeItem, List::addAll);
    log.info("Packed {} items into {} bins", items.size(), bins.size());
    return bins;
  }

  /**
   * Place {@code item} into the first bin that can hold it; if none, open a fresh bin. Mutates
   * {@code bins} — used as the accumulator step of the {@code pack} fold.
   */
  private void placeItem(List<Bin<T>> bins, T item) {
    bins.stream()
        .filter(b -> b.fits(item, maxWeightPerBin, maxItemsPerBin))
        .findFirst()
        .ifPresentOrElse(
            b -> b.add(item),
            () -> {
              Bin<T> fresh = new Bin<>();
              if (!fresh.fits(item, maxWeightPerBin, maxItemsPerBin)) {
                log.warn(
                    "Item exceeds per-bin caps on its own; placing in dedicated bin: weight={}",
                    item.getWeight());
              }
              fresh.add(item);
              bins.add(fresh);
            });
  }
}
