package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Bin packer that emits one bin per operation.
 *
 * <p>Ignores the {@link com.linkedin.openhouse.optimizer.model.TableStats} attached to each
 * candidate.
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
