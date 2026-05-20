package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FileCountBinPackerTest {

  private static final long MAX = 1_000_000L;
  private final FileCountBinPacker packer =
      new FileCountBinPacker(OperationTypeDto.ORPHAN_FILES_DELETION, MAX);

  private static TableOperationDto op(String uuid) {
    return TableOperationDto.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName("db")
        .tableName("tbl_" + uuid)
        .operationType(OperationTypeDto.ORPHAN_FILES_DELETION)
        .build();
  }

  private static TableStatsDto stats(Long fileCount) {
    return TableStatsDto.builder()
        .snapshot(TableStatsDto.SnapshotMetrics.builder().numCurrentFiles(fileCount).build())
        .build();
  }

  private static SchedulingCandidate candidate(String uuid, Long fileCount) {
    return new SchedulingCandidate(op(uuid), stats(fileCount));
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleTable_oneBin() {
    SchedulingCandidate c = candidate("uuid-1", 100L);
    List<Bin> bins = packer.pack(List.of(c));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).containsExactly(c.getOperation());
  }

  @Test
  void tablesUnderLimit_oneBin() {
    List<Bin> bins =
        packer.pack(
            List.of(candidate("a", 300_000L), candidate("b", 300_000L), candidate("c", 300_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).hasSize(3);
  }

  @Test
  void tablesOverLimit_twoBins() {
    List<Bin> bins =
        packer.pack(
            List.of(candidate("a", 600_000L), candidate("b", 600_000L), candidate("c", 400_000L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getOperations()).hasSize(2); // 600k + 400k
    assertThat(bins.get(1).getOperations()).hasSize(1); // 600k alone
  }

  @Test
  void largeTableAlone_exceedsLimitSingleBin() {
    SchedulingCandidate big = candidate("big", 5_000_000L);
    List<Bin> bins = packer.pack(List.of(big));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).containsExactly(big.getOperation());
  }

  @Test
  void nullFileCount_treatedAsZero() {
    List<Bin> bins = packer.pack(List.of(candidate("x", null), candidate("y", null)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).hasSize(2);
  }

  @Test
  void sortedDescending_largestFirst() {
    SchedulingCandidate small = candidate("small", 100L);
    SchedulingCandidate large = candidate("large", 900_000L);
    List<Bin> bins = packer.pack(List.of(small, large));
    assertThat(bins).hasSize(1);
    List<String> ordered =
        bins.get(0).getOperations().stream()
            .map(TableOperationDto::getTableUuid)
            .collect(Collectors.toList());
    assertThat(ordered).containsExactly("large", "small");
  }

  @Test
  void binCarriesOperationType() {
    List<Bin> bins = packer.pack(List.of(candidate("u", 1L)));
    assertThat(bins.get(0).getOperationType()).isEqualTo(OperationTypeDto.ORPHAN_FILES_DELETION);
  }
}
