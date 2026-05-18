package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FileCountBinPackerTest {

  private static final long MAX = 1_000_000L;
  private final FileCountBinPacker packer =
      new FileCountBinPacker(OperationType.ORPHAN_FILES_DELETION, MAX);

  private static TableOperation op(String uuid) {
    return TableOperation.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName("db")
        .tableName("tbl_" + uuid)
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .build();
  }

  private static TableStats stats(String uuid, Long fileCount) {
    return TableStats.builder()
        .tableUuid(uuid)
        .snapshot(TableStats.SnapshotMetrics.builder().numCurrentFiles(fileCount).build())
        .build();
  }

  private static Map<String, TableStats> statsMap(TableStats... entries) {
    Map<String, TableStats> m = new HashMap<>();
    for (TableStats s : entries) {
      m.put(s.getTableUuid(), s);
    }
    return m;
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    assertThat(packer.pack(List.of(), Map.of())).isEmpty();
  }

  @Test
  void singleTable_oneBin() {
    TableOperation o = op("uuid-1");
    List<Bin> bins = packer.pack(List.of(o), statsMap(stats("uuid-1", 100L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).containsExactly(o);
  }

  @Test
  void tablesUnderLimit_oneBin() {
    TableOperation a = op("a");
    TableOperation b = op("b");
    TableOperation c = op("c");
    List<Bin> bins =
        packer.pack(
            List.of(a, b, c),
            statsMap(stats("a", 300_000L), stats("b", 300_000L), stats("c", 300_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).hasSize(3);
  }

  @Test
  void tablesOverLimit_twoBins() {
    TableOperation a = op("a");
    TableOperation b = op("b");
    TableOperation c = op("c");
    List<Bin> bins =
        packer.pack(
            List.of(a, b, c),
            statsMap(stats("a", 600_000L), stats("b", 600_000L), stats("c", 400_000L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getOperations()).hasSize(2); // 600k + 400k
    assertThat(bins.get(1).getOperations()).hasSize(1); // 600k alone
  }

  @Test
  void largeTableAlone_exceedsLimitSingleBin() {
    TableOperation big = op("big");
    List<Bin> bins = packer.pack(List.of(big), statsMap(stats("big", 5_000_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).containsExactly(big);
  }

  @Test
  void missingStats_treatedAsZero() {
    TableOperation x = op("x");
    TableOperation y = op("y");
    List<Bin> bins = packer.pack(List.of(x, y), Map.of());
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).hasSize(2);
  }

  @Test
  void nullFileCount_treatedAsZero() {
    TableOperation x = op("x");
    TableOperation y = op("y");
    List<Bin> bins = packer.pack(List.of(x, y), statsMap(stats("x", null), stats("y", null)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getOperations()).hasSize(2);
  }

  @Test
  void sortedDescending_largestFirst() {
    TableOperation small = op("small");
    TableOperation large = op("large");
    List<Bin> bins =
        packer.pack(
            List.of(small, large), statsMap(stats("small", 100L), stats("large", 900_000L)));
    assertThat(bins).hasSize(1);
    List<String> ordered =
        bins.get(0).getOperations().stream()
            .map(TableOperation::getTableUuid)
            .collect(Collectors.toList());
    assertThat(ordered).containsExactly("large", "small");
  }

  @Test
  void binCarriesOperationType() {
    TableOperation o = op("u");
    List<Bin> bins = packer.pack(List.of(o), statsMap(stats("u", 1L)));
    assertThat(bins.get(0).getOperationType()).isEqualTo(OperationType.ORPHAN_FILES_DELETION);
  }
}
