package com.linkedin.openhouse.optimizer.binpack;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the operation-agnostic {@link FirstFitDecreasingBinPacker} via a {@link TestItem} that
 * implements {@link BinItem} but ignores the {@code fromOpAndStats} projection (returns {@code
 * this}) — this packer takes pre-projected items, so the projection method is irrelevant here.
 */
public class FirstFitDecreasingBinPackerTest {

  @Test
  public void emptyInputProducesEmptyOutput() {
    Assertions.assertTrue(packer(100, 50).pack(Collections.emptyList(), OFD).isEmpty());
  }

  @Test
  public void nullInputProducesEmptyOutput() {
    Assertions.assertTrue(packer(100, 50).pack(null, OFD).isEmpty());
  }

  @Test
  public void itemsSortDescendingByWeightBeforePacking() {
    List<BinItem> items =
        Arrays.asList(item("db.t_small", 10), item("db.t_big", 100), item("db.t_mid", 50));

    List<Bin> bins = packer(1000, 50).pack(items, OFD);

    Assertions.assertEquals(1, bins.size());
    Bin only = bins.get(0);
    Assertions.assertEquals(OFD, only.getOperationType());
    Assertions.assertEquals(3, only.getItems().size());
    Assertions.assertEquals("db.t_big", only.getItems().get(0).getFullyQualifiedTableName());
    Assertions.assertEquals("db.t_mid", only.getItems().get(1).getFullyQualifiedTableName());
    Assertions.assertEquals("db.t_small", only.getItems().get(2).getFullyQualifiedTableName());
  }

  @Test
  public void weightCapForcesMultipleBins() {
    List<BinItem> items =
        Arrays.asList(item("db.a", 60), item("db.b", 50), item("db.c", 40), item("db.d", 30));

    List<Bin> bins = packer(100, 50).pack(items, OFD);

    // FFD on [60, 50, 40, 30] with weight cap 100:
    //   bin0: 60          -> remaining 40
    //   bin0 tries 50 -> doesn't fit, new bin1: 50
    //   bin0 tries 40 -> fits, bin0: 60+40=100
    //   bin1 tries 30 -> fits, bin1: 50+30=80
    Assertions.assertEquals(2, bins.size());
    Assertions.assertEquals(
        100L, bins.get(0).getItems().stream().mapToLong(BinItem::getWeight).sum());
    Assertions.assertEquals(
        80L, bins.get(1).getItems().stream().mapToLong(BinItem::getWeight).sum());
  }

  @Test
  public void maxItemsPerBinCapHonored() {
    List<BinItem> items =
        IntStream.range(0, 5).mapToObj(i -> item("db.t" + i, 1)).collect(Collectors.toList());

    List<Bin> bins = packer(1000, 2).pack(items, OFD);

    Assertions.assertEquals(3, bins.size());
    Assertions.assertEquals(2, bins.get(0).getItems().size());
    Assertions.assertEquals(2, bins.get(1).getItems().size());
    Assertions.assertEquals(1, bins.get(2).getItems().size());
  }

  @Test
  public void oversizedItemGetsItsOwnBinRatherThanBeingDropped() {
    List<BinItem> items =
        Arrays.asList(item("db.tiny1", 10), item("db.giant", 500), item("db.tiny2", 10));

    List<Bin> bins = packer(100, 50).pack(items, OFD);

    long total =
        bins.stream().flatMap(b -> b.getItems().stream()).mapToLong(BinItem::getWeight).sum();
    Assertions.assertEquals(520L, total);
    boolean giantPresent =
        bins.stream()
            .flatMap(b -> b.getItems().stream())
            .anyMatch(i -> i.getFullyQualifiedTableName().equals("db.giant"));
    Assertions.assertTrue(giantPresent, "oversized item must not be dropped");
  }

  @Test
  public void disabledCapsLetEverythingShareOneBin() {
    List<BinItem> items =
        IntStream.range(0, 20).mapToObj(i -> item("db.t" + i, 100)).collect(Collectors.toList());

    List<Bin> bins = packer(0, 0).pack(items, OFD);

    Assertions.assertEquals(1, bins.size());
    Assertions.assertEquals(20, bins.get(0).getItems().size());
  }

  @Test
  public void operationTypeIsPropagatedToEachBin() {
    List<BinItem> items = Arrays.asList(item("db.a", 1), item("db.b", 1), item("db.c", 1));
    List<Bin> bins = packer(0, 2).pack(items, OFD);
    Assertions.assertEquals(2, bins.size());
    bins.forEach(b -> Assertions.assertEquals(OFD, b.getOperationType()));
  }

  private static final OperationTypeDto OFD = OperationTypeDto.ORPHAN_FILES_DELETION;

  private static FirstFitDecreasingBinPacker packer(long maxWeight, int maxItems) {
    return FirstFitDecreasingBinPacker.builder()
        .maxWeightPerBin(maxWeight)
        .maxItemsPerBin(maxItems)
        .build();
  }

  private static BinItem item(String fqtn, long weight) {
    return new TestItem(fqtn, "op-" + fqtn, weight);
  }

  /**
   * Minimal {@link BinItem} for testing — packer treats items as opaque, so projection is a no-op.
   */
  @AllArgsConstructor
  @Getter
  static class TestItem implements BinItem {
    private final String fullyQualifiedTableName;
    private final String operationId;
    private final long weight;

    @Override
    public BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats) {
      return this;
    }
  }
}
