package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link FirstFitBinPacker} bucketing logic in isolation via a test-only subclass that
 * projects to {@link TestItem}s with caller-controlled weights. Per-subtype projection logic (e.g.
 * {@link TotalFilesFirstFitBinPacker}) is covered by its own test.
 */
class FirstFitBinPackerTest {

  @AllArgsConstructor
  @Getter
  static class TestItem implements BinItem {
    private final String operationId;
    private final long weight;

    @Override
    public String getFullyQualifiedTableName() {
      return "db.tbl_" + operationId;
    }
  }

  /** Reads the weight from a single-entry tableProperties map keyed by {@code "weight"}. */
  static class TestBinPacker extends FirstFitBinPacker<TestItem> {
    TestBinPacker(long maxWeightPerBin, int maxItemsPerBin) {
      super(maxWeightPerBin, maxItemsPerBin);
    }

    @Override
    protected TestItem create(TableOperationDto operation, TableStatsDto stats) {
      long weight = Long.parseLong(stats.getTableProperties().get("weight"));
      return new TestItem(operation.getId(), weight);
    }
  }

  private static TableOperationDto op(String id) {
    return TableOperationDto.builder().id(id).tableUuid(id).build();
  }

  private static TableStatsDto statsWithWeight(String uuid, long weight) {
    return TableStatsDto.builder()
        .tableUuid(uuid)
        .tableProperties(Map.of("weight", Long.toString(weight)))
        .build();
  }

  private static List<TableOperationDto> opsList(String... ids) {
    return java.util.Arrays.stream(ids).map(FirstFitBinPackerTest::op).collect(Collectors.toList());
  }

  private static Map<String, TableStatsDto> statsMap(Object... uuidWeightPairs) {
    Map<String, TableStatsDto> map = new java.util.HashMap<>();
    for (int i = 0; i < uuidWeightPairs.length; i += 2) {
      String uuid = (String) uuidWeightPairs[i];
      long weight = (long) uuidWeightPairs[i + 1];
      map.put(uuid, statsWithWeight(uuid, weight));
    }
    return map;
  }

  @Test
  void emptyInput_returnsEmptyGroupings() {
    TestBinPacker packer = new TestBinPacker(100L, 10);
    assertThat(packer.pack(List.of(), Map.of())).isEmpty();
  }

  @Test
  void singleItem_oneGrouping() {
    TestBinPacker packer = new TestBinPacker(1_000_000L, 10);
    List<List<BinItem>> groupings = packer.pack(opsList("a"), statsMap("a", 100L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void underWeightLimit_oneGrouping() {
    TestBinPacker packer = new TestBinPacker(1_000_000L, 10);
    List<List<BinItem>> groupings =
        packer.pack(opsList("a", "b", "c"), statsMap("a", 300_000L, "b", 300_000L, "c", 300_000L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(3);
  }

  @Test
  void overWeightLimit_twoGroupings() {
    TestBinPacker packer = new TestBinPacker(1_000_000L, 10);
    List<List<BinItem>> groupings =
        packer.pack(opsList("a", "b", "c"), statsMap("a", 600_000L, "b", 600_000L, "c", 400_000L));
    assertThat(groupings).hasSize(2);
    // FFD: sort desc → 600, 600, 400. Place 600 → bin0; next 600 doesn't fit bin0, → bin1; 400
    // fits bin0 (total 1_000_000).
    long b0 = groupings.get(0).stream().mapToLong(BinItem::getWeight).sum();
    long b1 = groupings.get(1).stream().mapToLong(BinItem::getWeight).sum();
    assertThat(b0).isEqualTo(1_000_000L);
    assertThat(b1).isEqualTo(600_000L);
  }

  @Test
  void itemLargerThanCap_getsOwnGrouping() {
    TestBinPacker packer = new TestBinPacker(1_000L, 10);
    List<List<BinItem>> groupings = packer.pack(opsList("big"), statsMap("big", 5_000L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    TestBinPacker packer = new TestBinPacker(2_000_000L, 10);
    List<List<BinItem>> groupings =
        packer.pack(opsList("small", "large"), statsMap("small", 100L, "large", 900_000L));
    assertThat(groupings).hasSize(1);
    List<String> ids =
        groupings.get(0).stream().map(BinItem::getOperationId).collect(Collectors.toList());
    assertThat(ids).containsExactly("large", "small");
  }

  @Test
  void maxItemsCap_splitsGroupings() {
    TestBinPacker packer = new TestBinPacker(1_000_000L, 2);
    List<List<BinItem>> groupings =
        packer.pack(opsList("a", "b", "c", "d"), statsMap("a", 1L, "b", 1L, "c", 1L, "d", 1L));
    assertThat(groupings).hasSize(2);
    assertThat(groupings.get(0)).hasSize(2);
    assertThat(groupings.get(1)).hasSize(2);
  }

  @Test
  void operationsWithoutStats_dropped() {
    TestBinPacker packer = new TestBinPacker(1_000_000L, 10);
    List<List<BinItem>> groupings = packer.pack(opsList("a", "missing"), statsMap("a", 100L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
    assertThat(groupings.get(0).get(0).getOperationId()).isEqualTo("a");
  }
}
