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
 * Greedy first-fit-descending bin-packer keyed on per-table file count, projected from each
 * candidate's {@link TableStats}.
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

    // First-fit-descending is inherently stateful — each placement depends on the running totals
    // for bins assembled so far.
    List<List<TableOperation>> binContents = new ArrayList<>();
    List<Long> binTotals = new ArrayList<>();
    sorted.forEach(
        op -> {
          long c = costByOperationId.get(op.getId());
          OptionalInt placed =
              IntStream.range(0, binContents.size())
                  .filter(i -> binTotals.get(i) + c <= maxFilesPerBin || binTotals.get(i) == 0)
                  .findFirst();
          if (placed.isPresent()) {
            int idx = placed.getAsInt();
            binContents.get(idx).add(op);
            binTotals.set(idx, binTotals.get(idx) + c);
          } else {
            List<TableOperation> newBin = new ArrayList<>();
            newBin.add(op);
            binContents.add(newBin);
            binTotals.add(c);
          }
        });

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
