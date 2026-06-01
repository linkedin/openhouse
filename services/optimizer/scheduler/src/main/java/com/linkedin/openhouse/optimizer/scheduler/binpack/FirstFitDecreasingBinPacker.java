package com.linkedin.openhouse.optimizer.scheduler.binpack;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing bin packer with three independent caps:
 *
 * <ul>
 *   <li>{@code maxWeightPerBin} — total {@link BinItem#getWeight()} (for OFD: file count)
 *   <li>{@code maxSizeBytesPerBin} — total on-disk size of all items in the bin
 *   <li>{@code maxItemsPerBin} — number of items per bin
 * </ul>
 *
 * <p>Pass {@code 0} or a negative value for any cap to disable that dimension.
 *
 * <p>An item that exceeds any single cap on its own is placed into a bin by itself rather than
 * dropped — the scheduler never silently skips maintenance work for an oversized table.
 *
 * <p>Structurally mirrors {@code jobs.util.binpack.FirstFitDecreasingBinPacker} from PR&nbsp;#599.
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker implements BinPacker {

  @Builder.Default private final long maxWeightPerBin = 1_000_000L;
  @Builder.Default private final long maxSizeBytesPerBin = 5L * 1024L * 1024L * 1024L * 1024L;
  @Builder.Default private final int maxItemsPerBin = 50;

  @Override
  public List<Bin> pack(List<BinItem> items) {
    if (items == null || items.isEmpty()) {
      return new ArrayList<>();
    }

    List<BinItem> sorted =
        items.stream()
            .sorted(Comparator.comparingLong(BinItem::getWeight).reversed())
            .collect(Collectors.toList());

    List<Bin> bins = new ArrayList<>();
    for (BinItem item : sorted) {
      Bin target = null;
      for (Bin bin : bins) {
        if (bin.fits(item, maxWeightPerBin, maxSizeBytesPerBin, maxItemsPerBin)) {
          target = bin;
          break;
        }
      }
      if (target == null) {
        target = new Bin();
        bins.add(target);
        if (!target.fits(item, maxWeightPerBin, maxSizeBytesPerBin, maxItemsPerBin)) {
          log.warn(
              "Item exceeds per-bin caps on its own; placing in dedicated bin: fqtn={} weight={} sizeBytes={}",
              item.getFqtn(),
              item.getWeight(),
              item.getSizeBytes());
        }
      }
      target.add(item);
    }
    log.info("Packed {} items into {} bins", items.size(), bins.size());
    return bins;
  }
}
