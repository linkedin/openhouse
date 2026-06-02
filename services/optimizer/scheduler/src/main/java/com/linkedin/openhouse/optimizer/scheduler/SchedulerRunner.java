package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.scheduler.binpack.Bin;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Looks up the {@link BinPacker} registered for an operation type, asks it to prepare the bins for
 * this cycle, and lets each bin schedule itself. The runner holds an immutable {@code
 * OperationTypeDto -> BinPacker} map populated at construction by Spring injection; it doesn't know
 * which operations exist beyond what's in that map.
 */
@Slf4j
@Component
public class SchedulerRunner {
  private final Map<OperationTypeDto, BinPacker> binPackers;

  @Autowired
  public SchedulerRunner(List<BinPacker> binPackers) {
    this.binPackers =
        Map.copyOf(
            binPackers.stream()
                .collect(Collectors.toMap(BinPacker::getOperationType, Function.identity())));
  }

  public void schedule(OperationTypeDto type) {
    schedule(type, Optional.empty(), Optional.empty());
  }

  public void schedule(
      OperationTypeDto type, Optional<String> databaseName, Optional<String> tableName) {
    BinPacker packer = binPackers.get(type);
    if (packer == null) {
      throw new IllegalStateException("No BinPacker registered for operation type " + type);
    }
    packer.prepare(databaseName, tableName).forEach(Bin::schedule);
  }

  public Set<OperationTypeDto> getRegisteredOperationTypes() {
    return binPackers.keySet();
  }
}
