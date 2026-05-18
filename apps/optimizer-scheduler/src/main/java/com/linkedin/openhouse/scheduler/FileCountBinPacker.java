package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
    Map<String, Long> costByOperationId =
        pending.stream()
            .collect(Collectors.toMap(c -> c.getOperation().getId(), c -> cost(c.getStats())));

    List<TableOperation> sorted =
        pending.stream()
            .map(SchedulingCandidate::getOperation)
            .sorted(
                Comparator.comparingLong((TableOperation op) -> costByOperationId.get(op.getId()))
                    .reversed())
            .collect(Collectors.toList());

    // First-fit-descending is inherently stateful — each placement depends on running totals
    // for the bins assembled so far. Two parallel lists carry that state; sorted.forEach mutates
    // them via the helper.
    List<List<TableOperation>> binContents = new ArrayList<>();
    List<Long> binTotals = new ArrayList<>();
    sorted.forEach(op -> placeInBin(op, costByOperationId.get(op.getId()), binContents, binTotals));

    return binContents.stream()
        .map(ops -> new Bin(operationType, ops))
        .collect(Collectors.toList());
  }

  private void placeInBin(
      TableOperation op, long cost, List<List<TableOperation>> bins, List<Long> totals) {
    OptionalInt placed =
        IntStream.range(0, bins.size())
            .filter(i -> totals.get(i) + cost <= maxFilesPerBin || totals.get(i) == 0)
            .findFirst();
    if (placed.isPresent()) {
      int idx = placed.getAsInt();
      bins.get(idx).add(op);
      totals.set(idx, totals.get(idx) + cost);
    } else {
      List<TableOperation> newBin = new ArrayList<>();
      newBin.add(op);
      bins.add(newBin);
      totals.add(cost);
    }
  }

  private static long cost(TableStats stats) {
    if (stats == null || stats.getSnapshot() == null) {
      return 0L;
    }
    Long n = stats.getSnapshot().getNumCurrentFiles();
    return n != null ? n : 0L;
  }
}
