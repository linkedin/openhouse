package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Bin packer that emits one bin per operation — no batching. Suitable for operation types whose
 * Spark jobs aren't safe (or worth) running on multiple tables in a single submission, e.g.
 * snapshot expiration.
 *
 * <p>Ignores the {@link com.linkedin.openhouse.optimizer.model.TableStats} attached to each
 * candidate: cost has no effect on the packing decision because every operation is its own bin.
 */
@RequiredArgsConstructor
public class SingletonBinPacker implements BinPacker {

  private final OperationType operationType;

  @Override
  public List<Bin> pack(List<SchedulingCandidate> pending) {
    return pending.stream()
        .map(c -> new Bin(operationType, List.of(c.getOperation())))
        .collect(Collectors.toList());
  }
}
