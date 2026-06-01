package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;

class FirstFitDecreasingBinPackerTest {

  @AllArgsConstructor
  @Getter
  static class TestItem implements BinItem {
    private final String id;
    private final long weight;
  }

  private static TestItem item(String id, long weight) {
    return new TestItem(id, weight);
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().build();
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleItem_oneBin() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().maxWeightPerBin(1_000_000L).build();
    List<Bin<TestItem>> bins = packer.pack(List.of(item("a", 100L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(1);
  }

  @Test
  void underWeightLimit_oneBin() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().maxWeightPerBin(1_000_000L).build();
    List<Bin<TestItem>> bins =
        packer.pack(List.of(item("a", 300_000L), item("b", 300_000L), item("c", 300_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(3);
    assertThat(bins.get(0).getTotalWeight()).isEqualTo(900_000L);
  }

  @Test
  void overWeightLimit_twoBins() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().maxWeightPerBin(1_000_000L).build();
    List<Bin<TestItem>> bins =
        packer.pack(List.of(item("a", 600_000L), item("b", 600_000L), item("c", 400_000L)));
    assertThat(bins).hasSize(2);
    // FFD: sort desc → 600, 600, 400. Place 600 → bin0; next 600 doesn't fit bin0, → bin1;
    // 400 fits bin0 (total 1_000_000).
    assertThat(bins.get(0).getTotalWeight()).isEqualTo(1_000_000L);
    assertThat(bins.get(1).getTotalWeight()).isEqualTo(600_000L);
  }

  @Test
  void itemLargerThanCap_getsOwnBin() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().maxWeightPerBin(1_000L).build();
    List<Bin<TestItem>> bins = packer.pack(List.of(item("big", 5_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder().build();
    List<Bin<TestItem>> bins = packer.pack(List.of(item("small", 100L), item("large", 900_000L)));
    assertThat(bins).hasSize(1);
    List<String> ids =
        bins.get(0).items().stream().map(TestItem::getId).collect(Collectors.toList());
    assertThat(ids).containsExactly("large", "small");
  }

  @Test
  void maxItemsCap_splitsBins() {
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder()
            .maxWeightPerBin(0L)
            .maxItemsPerBin(2)
            .build();
    List<Bin<TestItem>> bins =
        packer.pack(List.of(item("a", 1L), item("b", 1L), item("c", 1L), item("d", 1L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).size()).isEqualTo(2);
    assertThat(bins.get(1).size()).isEqualTo(2);
  }

  @Test
  void zeroCap_disablesDimension() {
    // All caps zero → everything in one bin regardless of weight.
    FirstFitDecreasingBinPacker<TestItem> packer =
        FirstFitDecreasingBinPacker.<TestItem>builder()
            .maxWeightPerBin(0L)
            .maxItemsPerBin(0)
            .build();
    List<Bin<TestItem>> bins =
        packer.pack(List.of(item("a", Long.MAX_VALUE / 4), item("b", Long.MAX_VALUE / 4)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).size()).isEqualTo(2);
  }
}
