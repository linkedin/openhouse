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
  void emptyInput_returnsEmptyGroupings() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(100L).maxItemsPerBin(10).build();
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleItem_oneGrouping() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(1_000_000L)
            .maxItemsPerBin(10)
            .build();
    List<List<BinItem>> groupings = packer.pack(List.of(item("a", 100L)));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void underWeightLimit_oneGrouping() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(1_000_000L)
            .maxItemsPerBin(10)
            .build();
    List<List<BinItem>> groupings =
        packer.pack(List.of(item("a", 300_000L), item("b", 300_000L), item("c", 300_000L)));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(3);
    long total = groupings.get(0).stream().mapToLong(BinItem::getWeight).sum();
    assertThat(total).isEqualTo(900_000L);
  }

  @Test
  void overWeightLimit_twoGroupings() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(1_000_000L)
            .maxItemsPerBin(10)
            .build();
    List<List<BinItem>> groupings =
        packer.pack(List.of(item("a", 600_000L), item("b", 600_000L), item("c", 400_000L)));
    assertThat(groupings).hasSize(2);
    // FFD: sort desc → 600, 600, 400. Place 600 → group0; next 600 doesn't fit group0 → group1;
    // 400 fits group0 (total 1_000_000).
    long g0Total = groupings.get(0).stream().mapToLong(BinItem::getWeight).sum();
    long g1Total = groupings.get(1).stream().mapToLong(BinItem::getWeight).sum();
    assertThat(g0Total).isEqualTo(1_000_000L);
    assertThat(g1Total).isEqualTo(600_000L);
  }

  @Test
  void itemLargerThanCap_getsOwnGrouping() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000L).maxItemsPerBin(10).build();
    List<List<BinItem>> groupings = packer.pack(List.of(item("big", 5_000L)));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(2_000_000L)
            .maxItemsPerBin(10)
            .build();
    List<List<BinItem>> groupings =
        packer.pack(List.of(item("small", 100L), item("large", 900_000L)));
    assertThat(groupings).hasSize(1);
    List<String> ids =
        groupings.get(0).stream()
            .map(TestItem.class::cast)
            .map(TestItem::getId)
            .collect(Collectors.toList());
    assertThat(ids).containsExactly("large", "small");
  }

  @Test
  void maxItemsCap_splitsGroupings() {
    FirstFitDecreasingBinPacker packer =
        FirstFitDecreasingBinPacker.builder().maxWeightPerBin(1_000_000L).maxItemsPerBin(2).build();
    List<List<BinItem>> groupings =
        packer.pack(List.of(item("a", 1L), item("b", 1L), item("c", 1L), item("d", 1L)));
    assertThat(groupings).hasSize(2);
    assertThat(groupings.get(0)).hasSize(2);
    assertThat(groupings.get(1)).hasSize(2);
  }
}
