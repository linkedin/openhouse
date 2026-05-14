package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class FileCountBinPackerTest {

  private static final long MAX = 1_000_000L;
  private final FileCountBinPacker packer = new FileCountBinPacker(MAX);

  private static TableOperation op(String uuid, Long fileCount) {
    return TableOperation.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName("db")
        .tableName("tbl_" + uuid)
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .fileCount(fileCount)
        .build();
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleTable_oneBin() {
    TableOperation o = op("uuid-1", 100L);
    List<List<TableOperation>> bins = packer.pack(List.of(o));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).containsExactly(o);
  }

  @Test
  void tablesUnderLimit_oneBin() {
    TableOperation a = op("a", 300_000L);
    TableOperation b = op("b", 300_000L);
    TableOperation c = op("c", 300_000L);
    List<List<TableOperation>> bins = packer.pack(List.of(a, b, c));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).hasSize(3);
  }

  @Test
  void tablesOverLimit_twoBins() {
    TableOperation a = op("a", 600_000L);
    TableOperation b = op("b", 600_000L);
    TableOperation c = op("c", 400_000L);
    List<List<TableOperation>> bins = packer.pack(List.of(a, b, c));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0)).hasSize(2); // 600k + 400k
    assertThat(bins.get(1)).hasSize(1); // 600k alone
  }

  @Test
  void largeTableAlone_exceedsLimitSingleBin() {
    TableOperation big = op("big", 5_000_000L);
    List<List<TableOperation>> bins = packer.pack(List.of(big));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).containsExactly(big);
  }

  @Test
  void nullFileCount_treatedAsZero() {
    TableOperation x = op("x", null);
    TableOperation y = op("y", null);
    List<List<TableOperation>> bins = packer.pack(List.of(x, y));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).hasSize(2);
  }

  @Test
  void sortedDescending_largestFirst() {
    TableOperation small = op("small", 100L);
    TableOperation large = op("large", 900_000L);
    List<List<TableOperation>> bins = packer.pack(List.of(small, large));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).get(0).getTableUuid()).isEqualTo("large");
    assertThat(bins.get(0).get(1).getTableUuid()).isEqualTo("small");
  }
}
