package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Greedy first-fit-descending bin-packer keyed on per-table file count. File count is projected
 * from each candidate's {@link TableStats}; candidates whose stats are {@code null} or whose
 * snapshot reports no file count are treated as zero.
 *
 * <p>Candidates are sorted by descending file count, then assigned to the first bin whose running
 * total stays at or below {@code maxFilesPerBin}. An operation larger than the limit gets its own
 * bin (oversized bins are allowed — we never drop an operation).
 */
@RequiredArgsConstructor
public class FileCountBinPacker implements BinPacker {

  private final OperationType operationType;
  private final long maxFilesPerBin;

  @Override
  public List<Bin> pack(List<SchedulingCandidate> pending) {
    if (pending.isEmpty()) {
      return List.of();
    }

    // Project once: each candidate's packing cost is just a long, keyed by operation id.
    Map<String, Long> costByOperationId = new HashMap<>();
    for (SchedulingCandidate c : pending) {
      costByOperationId.put(c.getOperation().getId(), cost(c.getStats()));
    }

    List<TableOperation> sorted =
        pending.stream()
            .map(SchedulingCandidate::getOperation)
            .sorted(
                Comparator.comparingLong((TableOperation op) -> costByOperationId.get(op.getId()))
                    .reversed())
            .collect(Collectors.toList());

    List<List<TableOperation>> binContents = new ArrayList<>();
    List<Long> binTotals = new ArrayList<>();

    for (TableOperation op : sorted) {
      long cost = costByOperationId.get(op.getId());
      int placed = -1;
      for (int i = 0; i < binContents.size(); i++) {
        if (binTotals.get(i) + cost <= maxFilesPerBin || binTotals.get(i) == 0) {
          placed = i;
          break;
        }
      }
      if (placed >= 0) {
        binContents.get(placed).add(op);
        binTotals.set(placed, binTotals.get(placed) + cost);
      } else {
        List<TableOperation> newBin = new ArrayList<>();
        newBin.add(op);
        binContents.add(newBin);
        binTotals.add(cost);
      }
    }

    return binContents.stream()
        .map(ops -> new Bin(operationType, ops))
        .collect(Collectors.toList());
  }

  private static long cost(TableStats stats) {
    if (stats == null || stats.getSnapshot() == null) {
      return 0L;
    }
    Long n = stats.getSnapshot().getNumCurrentFiles();
    return n != null ? n : 0L;
  }
}
