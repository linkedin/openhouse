package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BinPackerTest {

  private static TableOperationRow row(String uuid) {
    return TableOperationRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName("db")
        .tableName("tbl_" + uuid)
        .operationType("ORPHAN_FILES_DELETION")
        .status("PENDING")
        .build();
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    assertThat(BinPacker.pack(List.of(), Map.of(), 1_000_000L)).isEmpty();
  }

  @Test
  void singleTable_oneBin() {
    TableOperationRow r = row("uuid-1");
    List<List<TableOperationRow>> bins =
        BinPacker.pack(List.of(r), Map.of("uuid-1", 100L), 1_000_000L);

    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).containsExactly(r);
  }

  @Test
  void tablesUnderLimit_oneBin() {
    TableOperationRow r1 = row("a");
    TableOperationRow r2 = row("b");
    TableOperationRow r3 = row("c");

    List<List<TableOperationRow>> bins =
        BinPacker.pack(
            List.of(r1, r2, r3), Map.of("a", 300_000L, "b", 300_000L, "c", 300_000L), 1_000_000L);

    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).hasSize(3);
  }

  @Test
  void tablesOverLimit_twoBins() {
    TableOperationRow r1 = row("a");
    TableOperationRow r2 = row("b");
    TableOperationRow r3 = row("c");

    // 600k + 600k would exceed 1M; 400k fits after 600k
    List<List<TableOperationRow>> bins =
        BinPacker.pack(
            List.of(r1, r2, r3), Map.of("a", 600_000L, "b", 600_000L, "c", 400_000L), 1_000_000L);

    assertThat(bins).hasSize(2);
    assertThat(bins.get(0)).hasSize(2); // 600k + 400k
    assertThat(bins.get(1)).hasSize(1); // 600k alone
  }

  @Test
  void largeTableAlone_exceedsLimitSingleBin() {
    TableOperationRow r = row("big");
    List<List<TableOperationRow>> bins =
        BinPacker.pack(List.of(r), Map.of("big", 5_000_000L), 1_000_000L);

    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).containsExactly(r);
  }

  @Test
  void noStats_fileCountZero_groupedNormally() {
    TableOperationRow r1 = row("x");
    TableOperationRow r2 = row("y");

    // No stats entries — both get cost 0, both fit in one bin
    List<List<TableOperationRow>> bins = BinPacker.pack(List.of(r1, r2), Map.of(), 1_000_000L);

    assertThat(bins).hasSize(1);
    assertThat(bins.get(0)).hasSize(2);
  }

  @Test
  void sortedDescending_largestTablesFirst() {
    TableOperationRow small = row("small");
    TableOperationRow large = row("large");

    List<List<TableOperationRow>> bins =
        BinPacker.pack(List.of(small, large), Map.of("small", 100L, "large", 900_000L), 1_000_000L);

    // Both fit in one bin, large should appear first due to descending sort
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).get(0).getTableUuid()).isEqualTo("large");
    assertThat(bins.get(0).get(1).getTableUuid()).isEqualTo("small");
  }
}
