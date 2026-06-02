package com.linkedin.openhouse.optimizer.scheduler;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.scheduler.binpack.Bin;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchedulerRunnerTest {

  @Mock private BinPacker packer;
  @Mock private Bin bin1;
  @Mock private Bin bin2;

  @Test
  void schedule_unknownOperationType_throws() {
    SchedulerRunner runner = new SchedulerRunner(List.of());

    assertThatThrownBy(() -> runner.schedule(OperationTypeDto.ORPHAN_FILES_DELETION))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No BinPacker registered");
  }

  @Test
  void schedule_delegatesToPackerAndSchedulesEachBin() {
    when(packer.getOperationType()).thenReturn(OperationTypeDto.ORPHAN_FILES_DELETION);
    when(packer.prepare(any(), any())).thenReturn(List.of(bin1, bin2));

    SchedulerRunner runner = new SchedulerRunner(List.of(packer));
    runner.schedule(OperationTypeDto.ORPHAN_FILES_DELETION);

    verify(packer).prepare(eq(Optional.empty()), eq(Optional.empty()));
    verify(bin1, times(1)).schedule();
    verify(bin2, times(1)).schedule();
  }

  @Test
  void schedule_passesScopeArgsThrough() {
    when(packer.getOperationType()).thenReturn(OperationTypeDto.ORPHAN_FILES_DELETION);
    when(packer.prepare(any(), any())).thenReturn(List.of());

    SchedulerRunner runner = new SchedulerRunner(List.of(packer));
    runner.schedule(OperationTypeDto.ORPHAN_FILES_DELETION, Optional.of("db1"), Optional.of("t1"));

    verify(packer).prepare(eq(Optional.of("db1")), eq(Optional.of("t1")));
  }
}
