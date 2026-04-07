package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Greedy first-fit descending bin-packer for table operations.
 *
 * <p>Tables are sorted by descending file count, then assigned to the first bin whose running total
 * stays below {@code maxFilesPerBin}. A table larger than the limit gets its own bin (oversized
 * bins are allowed — we never drop a table).
 *
 * <p>Tables with no stats entry are treated as file count = 0 and are still schedulable.
 */
public final class BinPacker {

  private BinPacker() {}

  /**
   * Pack {@code pending} rows into bins.
   *
   * @param pending operations to pack; must not be empty
   * @param fileCountByUuid map from tableUuid to number of current data files; missing entries
   *     default to 0
   * @param maxFilesPerBin maximum total file count per bin
   * @return list of bins, each bin being a non-empty list of rows
   */
  public static List<List<TableOperationRow>> pack(
      List<TableOperationRow> pending, Map<String, Long> fileCountByUuid, long maxFilesPerBin) {

    if (pending.isEmpty()) {
      return List.of();
    }

    List<TableOperationRow> sorted =
        pending.stream()
            .sorted(
                Comparator.comparingLong(
                        (TableOperationRow r) -> fileCountByUuid.getOrDefault(r.getTableUuid(), 0L))
                    .reversed())
            .collect(Collectors.toList());

    List<List<TableOperationRow>> bins = new ArrayList<>();
    List<Long> binTotals = new ArrayList<>();

    for (TableOperationRow row : sorted) {
      long cost = fileCountByUuid.getOrDefault(row.getTableUuid(), 0L);

      int placed = -1;
      for (int i = 0; i < bins.size(); i++) {
        if (binTotals.get(i) + cost <= maxFilesPerBin || binTotals.get(i) == 0) {
          placed = i;
          break;
        }
      }

      if (placed >= 0) {
        bins.get(placed).add(row);
        binTotals.set(placed, binTotals.get(placed) + cost);
      } else {
        List<TableOperationRow> newBin = new ArrayList<>();
        newBin.add(row);
        bins.add(newBin);
        binTotals.add(cost);
      }
    }

    return bins;
  }
}
