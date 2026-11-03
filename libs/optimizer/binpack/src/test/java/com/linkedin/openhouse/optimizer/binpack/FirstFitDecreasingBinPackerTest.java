package com.linkedin.openhouse.optimizer.binpack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;

/**
 * Tests the three callable shapes of {@link FirstFitDecreasingBinPacker}:
 *
 * <ul>
 *   <li>{@code pack(ops, stats)} — projection path via {@link ProjectingTestItem}'s {@code
 *       fromOpAndStats}.
 *   <li>{@code pack(items)} — pre-projected raw groupings.
 *   <li>{@code pack(items, operationType)} — pre-projected tagged {@link Bin}s.
 * </ul>
 *
 * Projection logic for production BinItems (e.g. {@link TotalFilesBinItem}) is covered by their own
 * tests.
 */
class FirstFitDecreasingBinPackerTest {

  private static final OperationTypeDto OFD = OperationTypeDto.ORPHAN_FILES_DELETION;

  // -------------------- Projection path: pack(ops, stats) --------------------

  @Test
  void emptyInput_returnsEmptyGroupings() {
    assertThat(projectionPacker(100L, 10).pack(Collections.emptyList(), Collections.emptyMap()))
        .isEmpty();
  }

  @Test
  void singleItem_oneGrouping() {
    List<List<BinItem>> groupings =
        projectionPacker(1_000_000L, 10).pack(opsList("a"), statsMap("a", 100L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void underWeightLimit_oneGrouping() {
    List<List<BinItem>> groupings =
        projectionPacker(1_000_000L, 10)
            .pack(opsList("a", "b", "c"), statsMap("a", 300_000L, "b", 300_000L, "c", 300_000L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(3);
  }

  @Test
  void overWeightLimit_twoGroupings() {
    List<List<BinItem>> groupings =
        projectionPacker(1_000_000L, 10)
            .pack(opsList("a", "b", "c"), statsMap("a", 600_000L, "b", 600_000L, "c", 400_000L));
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
    List<List<BinItem>> groupings =
        projectionPacker(1_000L, 10).pack(opsList("big"), statsMap("big", 5_000L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    List<List<BinItem>> groupings =
        projectionPacker(2_000_000L, 10)
            .pack(opsList("small", "large"), statsMap("small", 100L, "large", 900_000L));
    assertThat(groupings).hasSize(1);
    List<String> ids =
        groupings.get(0).stream().map(BinItem::getOperationId).collect(Collectors.toList());
    assertThat(ids).containsExactly("large", "small");
  }

  @Test
  void maxItemsCap_splitsGroupings() {
    List<List<BinItem>> groupings =
        projectionPacker(1_000_000L, 2)
            .pack(opsList("a", "b", "c", "d"), statsMap("a", 1L, "b", 1L, "c", 1L, "d", 1L));
    assertThat(groupings).hasSize(2);
    assertThat(groupings.get(0)).hasSize(2);
    assertThat(groupings.get(1)).hasSize(2);
  }

  @Test
  void operationsWithoutStats_dropped() {
    List<List<BinItem>> groupings =
        projectionPacker(1_000_000L, 10).pack(opsList("a", "missing"), statsMap("a", 100L));
    assertThat(groupings).hasSize(1);
    assertThat(groupings.get(0)).hasSize(1);
    assertThat(groupings.get(0).get(0).getOperationId()).isEqualTo("a");
  }

  // -------------------- Pre-projected path: pack(items) and pack(items, OFD) --------------------

  @Test
  void preProjected_emptyInput_returnsEmpty() {
    assertThat(preProjectedPacker(100L, 50).pack(Collections.emptyList(), OFD)).isEmpty();
    assertThat(preProjectedPacker(100L, 50).pack((List<BinItem>) null, OFD)).isEmpty();
  }

  @Test
  void preProjected_itemsSortDescendingBeforePlacing() {
    List<BinItem> items =
        Arrays.asList(item("db.t_small", 10), item("db.t_big", 100), item("db.t_mid", 50));

    List<Bin> bins = preProjectedPacker(1000L, 50).pack(items, OFD);

    assertThat(bins).hasSize(1);
    Bin only = bins.get(0);
    assertThat(only.getOperationType()).isEqualTo(OFD);
    assertThat(only.getItems())
        .extracting(BinItem::getFullyQualifiedTableName)
        .containsExactly("db.t_big", "db.t_mid", "db.t_small");
  }

  @Test
  void preProjected_weightCapForcesMultipleBins() {
    List<BinItem> items =
        Arrays.asList(item("db.a", 60), item("db.b", 50), item("db.c", 40), item("db.d", 30));

    List<Bin> bins = preProjectedPacker(100L, 50).pack(items, OFD);

    // FFD on [60, 50, 40, 30] with weight cap 100:
    //   bin0: 60         → remaining 40
    //   bin0 tries 50 → doesn't fit, new bin1: 50
    //   bin0 tries 40 → fits, bin0: 60 + 40 = 100
    //   bin1 tries 30 → fits, bin1: 50 + 30 = 80
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getItems().stream().mapToLong(BinItem::getWeight).sum()).isEqualTo(100L);
    assertThat(bins.get(1).getItems().stream().mapToLong(BinItem::getWeight).sum()).isEqualTo(80L);
  }

  @Test
  void preProjected_maxItemsPerBinCapHonored() {
    List<BinItem> items =
        IntStream.range(0, 5).mapToObj(i -> item("db.t" + i, 1)).collect(Collectors.toList());

    List<Bin> bins = preProjectedPacker(1000L, 2).pack(items, OFD);

    assertThat(bins).hasSize(3);
    assertThat(bins.get(0).getItems()).hasSize(2);
    assertThat(bins.get(1).getItems()).hasSize(2);
    assertThat(bins.get(2).getItems()).hasSize(1);
  }

  @Test
  void preProjected_oversizedItemGetsItsOwnBin() {
    List<BinItem> items =
        Arrays.asList(item("db.tiny1", 10), item("db.giant", 500), item("db.tiny2", 10));

    List<Bin> bins = preProjectedPacker(100L, 50).pack(items, OFD);

    long total =
        bins.stream().flatMap(b -> b.getItems().stream()).mapToLong(BinItem::getWeight).sum();
    assertThat(total).isEqualTo(520L);
    boolean giantPresent =
        bins.stream()
            .flatMap(b -> b.getItems().stream())
            .anyMatch(i -> i.getFullyQualifiedTableName().equals("db.giant"));
    assertThat(giantPresent).isTrue();
  }

  @Test
  void preProjected_operationTypeIsPropagatedToEachBin() {
    List<BinItem> items = Arrays.asList(item("db.a", 1), item("db.b", 1), item("db.c", 1));
    List<Bin> bins = preProjectedPacker(1000L, 2).pack(items, OFD);
    assertThat(bins).hasSize(2);
    assertThat(bins).allMatch(b -> b.getOperationType().equals(OFD));
  }

  @Test
  void preProjected_rawGroupingsOverload_omitsOperationType() {
    List<BinItem> items = Arrays.asList(item("db.a", 60), item("db.b", 50));
    List<List<BinItem>> groupings = preProjectedPacker(100L, 50).pack(items);
    assertThat(groupings).hasSize(2);
    assertThat(groupings.get(0)).hasSize(1);
    assertThat(groupings.get(1)).hasSize(1);
  }

  // -------------------- Builder / contract checks --------------------

  @Test
  void omittingBinItemSupplier_rejectsProjectionPath() {
    FirstFitDecreasingBinPacker<BinItem> packer =
        FirstFitDecreasingBinPacker.<BinItem>builder()
            .maxWeightPerBin(100L)
            .maxItemsPerBin(10)
            .build();
    assertThatThrownBy(() -> packer.pack(Collections.emptyList(), Collections.emptyMap()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("binItemSupplier");
  }

  @Test
  void builderDefaults_appliedWhenCapsOmitted() {
    // The defaults are 1_000_000 weight / 50 items — sized for OFD. With 60 items of weight 100,
    // the 50-items cap kicks in before the weight cap.
    List<BinItem> items =
        IntStream.range(0, 60).mapToObj(i -> item("db.t" + i, 100)).collect(Collectors.toList());

    List<Bin> bins = FirstFitDecreasingBinPacker.<BinItem>builder().build().pack(items, OFD);

    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getItems()).hasSize(50);
    assertThat(bins.get(1).getItems()).hasSize(10);
  }

  // -------------------- Fixtures --------------------

  private static FirstFitDecreasingBinPacker<ProjectingTestItem> projectionPacker(
      long maxWeight, int maxItems) {
    return FirstFitDecreasingBinPacker.<ProjectingTestItem>builder()
        .binItemSupplier(ProjectingTestItem::new)
        .maxWeightPerBin(maxWeight)
        .maxItemsPerBin(maxItems)
        .build();
  }

  private static FirstFitDecreasingBinPacker<BinItem> preProjectedPacker(
      long maxWeight, int maxItems) {
    return FirstFitDecreasingBinPacker.<BinItem>builder()
        .maxWeightPerBin(maxWeight)
        .maxItemsPerBin(maxItems)
        .build();
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
    return Arrays.stream(ids).map(FirstFitDecreasingBinPackerTest::op).collect(Collectors.toList());
  }

  private static Map<String, TableStatsDto> statsMap(Object... uuidWeightPairs) {
    Map<String, TableStatsDto> map = new HashMap<>();
    for (int i = 0; i < uuidWeightPairs.length; i += 2) {
      String uuid = (String) uuidWeightPairs[i];
      long weight = (long) uuidWeightPairs[i + 1];
      map.put(uuid, statsWithWeight(uuid, weight));
    }
    return map;
  }

  private static BinItem item(String fqtn, long weight) {
    return new PreProjectedTestItem(fqtn, "op-" + fqtn, weight);
  }

  /** Exercises the {@code fromOpAndStats} projection callback. */
  @Getter
  static class ProjectingTestItem implements BinItem {
    private final String operationId;
    private final long weight;

    public ProjectingTestItem() {
      this("", 0L);
    }

    private ProjectingTestItem(String operationId, long weight) {
      this.operationId = operationId;
      this.weight = weight;
    }

    @Override
    public String getFullyQualifiedTableName() {
      return "db.tbl_" + operationId;
    }

    @Override
    public BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats) {
      long w = Long.parseLong(stats.getTableProperties().get("weight"));
      return new ProjectingTestItem(op.getId(), w);
    }
  }

  /** Pre-projected: packer treats items as opaque so {@code fromOpAndStats} is a no-op. */
  @AllArgsConstructor
  @Getter
  static class PreProjectedTestItem implements BinItem {
    private final String fullyQualifiedTableName;
    private final String operationId;
    private final long weight;

    @Override
    public BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats) {
      return this;
    }
  }
}
