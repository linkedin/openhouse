package com.linkedin.openhouse.jobs.util.binpack;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * First-fit-decreasing bin packer used by the optimizer scheduler to group table operations into
 * batches before launching a single Spark job per batch.
 *
 * <p>Each bin has three independent caps:
 *
 * <ul>
 *   <li>{@code maxWeightPerBin} — total {@link BinItem#getWeight()} (for OFD: number of files)
 *   <li>{@code maxSizeBytesPerBin} — total on-disk size of all tables in the bin
 *   <li>{@code maxItemsPerBin} — number of tables per bin
 * </ul>
 *
 * <p>An item that exceeds any single cap on its own is placed into a bin by itself rather than
 * dropped — we never silently skip maintenance work for an oversized table.
 *
 * <p>Pass {@code 0} or a negative value for any cap to disable that dimension.
 */
@Slf4j
@Builder
public class FirstFitDecreasingBinPacker {

  @Builder.Default private final long maxWeightPerBin = 1_000_000L;
  @Builder.Default private final long maxSizeBytesPerBin = 5L * 1024L * 1024L * 1024L * 1024L;
  @Builder.Default private final int maxItemsPerBin = 50;

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
