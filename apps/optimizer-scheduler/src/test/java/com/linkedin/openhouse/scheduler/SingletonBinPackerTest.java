package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SingletonBinPackerTest {

  private final SingletonBinPacker packer =
      new SingletonBinPacker(OperationType.SNAPSHOT_EXPIRATION);

  private static SchedulingCandidate candidate(String uuid) {
    TableOperation op =
        TableOperation.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(uuid)
            .databaseName("db")
            .tableName("tbl_" + uuid)
            .operationType(OperationType.SNAPSHOT_EXPIRATION)
            .build();
    TableStats stats =
        TableStats.builder()
            .tableUuid(uuid)
            .snapshot(TableStats.SnapshotMetrics.builder().numCurrentFiles(0L).build())
            .build();
    return new SchedulingCandidate(op, stats);
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void everyCandidateGetsItsOwnBin() {
    SchedulingCandidate a = candidate("a");
    SchedulingCandidate b = candidate("b");
    SchedulingCandidate c = candidate("c");

    List<Bin> bins = packer.pack(List.of(a, b, c));

    assertThat(bins).hasSize(3);
    bins.forEach(bin -> assertThat(bin.getOperations()).hasSize(1));
    List<String> uuids =
        bins.stream()
            .map(bin -> bin.getOperations().get(0).getTableUuid())
            .collect(Collectors.toList());
    assertThat(uuids).containsExactly("a", "b", "c");
  }

  @Test
  void binsCarryConfiguredOperationType() {
    List<Bin> bins = packer.pack(List.of(candidate("u")));
    assertThat(bins.get(0).getOperationType()).isEqualTo(OperationType.SNAPSHOT_EXPIRATION);
  }
}
