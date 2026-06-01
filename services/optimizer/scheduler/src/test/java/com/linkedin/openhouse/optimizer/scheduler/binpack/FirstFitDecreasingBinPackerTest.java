package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FirstFitDecreasingBinPackerTest {

  private static BinItem item(String id, long weight) {
    return item(id, weight, 0L);
  }

  private static BinItem item(String id, long weight, long sizeBytes) {
    return BinItem.builder()
        .fqtn("db.tbl_" + id)
        .operationId("op-" + id)
        .tableUuid("uuid-" + id)
        .databaseName("db")
        .tableName("tbl_" + id)
        .weight(weight)
        .sizeBytes(sizeBytes)
        .build();
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    FirstFitDecreasingBinPacker packer = FirstFitDecreasingBinPacker.builder().build();
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleItem_oneBin() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000_000L).build();
    List<Bin> bins = packer.pack(List.of(item("a", 100L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(1);
  }

  @Test
  void underWeightLimit_oneBin() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000_000L).build();
    List<Bin> bins =
        packer.pack(List.of(item("a", 300_000L), item("b", 300_000L), item("c", 300_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(3);
    assertThat(bins.get(0).getTotalWeight()).isEqualTo(900_000L);
  }

  @Test
  void overWeightLimit_twoBins() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000_000L).build();
    List<Bin> bins =
        packer.pack(List.of(item("a", 600_000L), item("b", 600_000L), item("c", 400_000L)));
    assertThat(bins).hasSize(2);
    // FFD: largest first, place 600k → bin0; next 600k doesn't fit bin0, → bin1; 400k fits bin0.
    assertThat(bins.get(0).getTotalWeight()).isEqualTo(1_000_000L);
    assertThat(bins.get(1).getTotalWeight()).isEqualTo(600_000L);
  }

  @Test
  void itemLargerThanCap_getsOwnBin() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000L).build();
    List<Bin> bins = packer.pack(List.of(item("big", 5_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    FirstFitDecreasingBinPacker packer = FirstFitDecreasingBinPacker.builder().build();
    List<Bin> bins = packer.pack(List.of(item("small", 100L), item("large", 900_000L)));
    assertThat(bins).hasSize(1);
    List<String> uuids =
        bins.get(0).items().stream().map(BinItem::getTableUuid).collect(Collectors.toList());
    assertThat(uuids).containsExactly("uuid-large", "uuid-small");
  }

  @Test
  void sizeBytesCap_splitsBins() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(0L) // disable
            .maxSizeBytesPerBin(1_000L)
            .maxItemsPerBin(0)
            .build();
    List<Bin> bins =
        packer.pack(List.of(item("a", 0L, 600L), item("b", 0L, 500L), item("c", 0L, 400L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getTotalSizeBytes()).isEqualTo(1_000L); // 600 + 400
    assertThat(bins.get(1).getTotalSizeBytes()).isEqualTo(500L);
  }

  @Test
  void maxItemsCap_splitsBins() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(0L)
            .maxSizeBytesPerBin(0L)
            .maxItemsPerBin(2)
            .build();
    List<Bin> bins =
        packer.pack(List.of(item("a", 1L), item("b", 1L), item("c", 1L), item("d", 1L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).size()).isEqualTo(2);
    assertThat(bins.get(1).size()).isEqualTo(2);
  }

  @Test
  void zeroCap_disablesDimension() {
    // All caps zero → everything in one bin regardless of weight/size.
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(0L)
            .maxSizeBytesPerBin(0L)
            .maxItemsPerBin(0)
            .build();
    List<Bin> bins =
        packer.pack(
            List.of(
                item("a", Long.MAX_VALUE / 4, Long.MAX_VALUE / 4),
                item("b", Long.MAX_VALUE / 4, Long.MAX_VALUE / 4)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(2);
  }
}
