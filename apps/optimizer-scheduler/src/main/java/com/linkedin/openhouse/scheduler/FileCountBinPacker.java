package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.TableOperation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Greedy first-fit-descending bin-packer keyed on {@link TableOperation#getFileCount()}.
 *
 * <p>Operations are sorted by descending file count, then assigned to the first bin whose running
 * total stays at or below {@code maxFilesPerBin}. An operation larger than the limit gets its own
 * bin (oversized bins are allowed — we never drop an operation).
 *
 * <p>Operations with no file-count populated are treated as zero.
 */
@RequiredArgsConstructor
public class FileCountBinPacker implements BinPacker {

  private final long maxFilesPerBin;

  @Override
  public List<List<TableOperation>> pack(List<TableOperation> pending) {
    if (pending.isEmpty()) {
      return List.of();
    }

    List<TableOperation> sorted =
        pending.stream()
            .sorted(Comparator.comparingLong(FileCountBinPacker::cost).reversed())
            .collect(Collectors.toList());

    List<List<TableOperation>> bins = new ArrayList<>();
    List<Long> binTotals = new ArrayList<>();

    for (TableOperation op : sorted) {
      long cost = cost(op);
      int placed = -1;
      for (int i = 0; i < bins.size(); i++) {
        if (binTotals.get(i) + cost <= maxFilesPerBin || binTotals.get(i) == 0) {
          placed = i;
          break;
        }
      }
      if (placed >= 0) {
        bins.get(placed).add(op);
        binTotals.set(placed, binTotals.get(placed) + cost);
      } else {
        List<TableOperation> newBin = new ArrayList<>();
        newBin.add(op);
        bins.add(newBin);
        binTotals.add(cost);
      }
    }
    return bins;
  }

  private static long cost(TableOperation op) {
    return op.getFileCount() != null ? op.getFileCount() : 0L;
  }
}
