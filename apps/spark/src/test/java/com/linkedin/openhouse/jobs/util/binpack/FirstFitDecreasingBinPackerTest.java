package com.linkedin.openhouse.jobs.util.binpack;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FirstFitDecreasingBinPackerTest {

  @Test
  public void emptyInputProducesEmptyOutput() {
    List<Bin> bins = packer(100, 0, 50).pack(Collections.emptyList());
    Assertions.assertTrue(bins.isEmpty());
  }

  @Test
  public void nullInputProducesEmptyOutput() {
    List<Bin> bins = packer(100, 0, 50).pack(null);
    Assertions.assertTrue(bins.isEmpty());
  }

  @Test
  public void itemsSortDescendingByWeightBeforePacking() {
    List<BinItem> items =
        Arrays.asList(item("db.t_small", 10), item("db.t_big", 100), item("db.t_mid", 50));

    List<Bin> bins = packer(1000, 0, 50).pack(items);

    // Everything fits in one bin since capacity is huge; order inside the bin must be descending.
    Assertions.assertEquals(1, bins.size());
    Bin only = bins.get(0);
    Assertions.assertEquals(3, only.size());
    Assertions.assertEquals("db.t_big", only.items().get(0).getFqtn());
    Assertions.assertEquals("db.t_mid", only.items().get(1).getFqtn());
    Assertions.assertEquals("db.t_small", only.items().get(2).getFqtn());
    Assertions.assertEquals(160, only.getTotalWeight());
  }

  @Test
  public void weightCapForcesMultipleBins() {
    List<BinItem> items =
        Arrays.asList(item("db.a", 60), item("db.b", 50), item("db.c", 40), item("db.d", 30));

    List<Bin> bins = packer(100, 0, 50).pack(items);

    // FFD on [60, 50, 40, 30] with cap 100:
    //   bin0: 60          -> remaining 40
    //   bin0 tries 50 -> doesn't fit, new bin1: 50
    //   bin0 tries 40 -> fits, bin0: 60+40=100
    //   bin1 tries 30 -> fits, bin1: 50+30=80
    Assertions.assertEquals(2, bins.size());
    Assertions.assertEquals(100, bins.get(0).getTotalWeight());
    Assertions.assertEquals(80, bins.get(1).getTotalWeight());
  }

  @Test
  public void maxItemsPerBinCapHonored() {
    List<BinItem> items =
        IntStream.range(0, 5).mapToObj(i -> item("db.t" + i, 1)).collect(Collectors.toList());

    List<Bin> bins = packer(1000, 0, 2).pack(items);

    Assertions.assertEquals(3, bins.size());
    Assertions.assertEquals(2, bins.get(0).size());
    Assertions.assertEquals(2, bins.get(1).size());
    Assertions.assertEquals(1, bins.get(2).size());
  }

  @Test
  public void maxSizeBytesCapHonored() {
    List<BinItem> items =
        Arrays.asList(
            BinItem.builder()
                .fqtn("db.a")
                .operationId("op-a")
                .tableUuid("uuid-a")
                .databaseName("db")
                .tableName("a")
                .weight(1)
                .sizeBytes(800L)
                .build(),
            BinItem.builder()
                .fqtn("db.b")
                .operationId("op-b")
                .tableUuid("uuid-b")
                .databaseName("db")
                .tableName("b")
                .weight(1)
                .sizeBytes(800L)
                .build());

    List<Bin> bins = packer(1000, 1000L, 50).pack(items);

    Assertions.assertEquals(2, bins.size());
    Assertions.assertEquals(800L, bins.get(0).getTotalSizeBytes());
    Assertions.assertEquals(800L, bins.get(1).getTotalSizeBytes());
  }

  @Test
  public void oversizedItemGetsItsOwnBinRatherThanBeingDropped() {
    List<BinItem> items =
        Arrays.asList(item("db.tiny1", 10), item("db.giant", 500), item("db.tiny2", 10));

    List<Bin> bins = packer(100, 0, 50).pack(items);

    // Giant exceeds the cap on its own — must still appear in some bin.
    long total = bins.stream().mapToLong(Bin::getTotalWeight).sum();
    Assertions.assertEquals(520, total);
    boolean giantPresent =
        bins.stream()
            .flatMap(b -> b.items().stream())
            .anyMatch(i -> i.getFqtn().equals("db.giant"));
    Assertions.assertTrue(giantPresent, "oversized item must not be dropped");
  }

  @Test
  public void disabledCapsLetEverythingShareOneBin() {
    List<BinItem> items =
        IntStream.range(0, 20).mapToObj(i -> item("db.t" + i, 100)).collect(Collectors.toList());

    List<Bin> bins = packer(0, 0, 0).pack(items);

    Assertions.assertEquals(1, bins.size());
    Assertions.assertEquals(20, bins.get(0).size());
  }

  private static FirstFitDecreasingBinPacker packer(long maxWeight, long maxSize, int maxItems) {
    return FirstFitDecreasingBinPacker.builder()
        .maxWeightPerBin(maxWeight)
        .maxSizeBytesPerBin(maxSize)
        .maxItemsPerBin(maxItems)
        .build();
  }

  private static BinItem item(String fqtn, long weight) {
    String[] parts = fqtn.split("\\.", 2);
    return BinItem.builder()
        .fqtn(fqtn)
        .operationId("op-" + parts[1])
        .tableUuid("uuid-" + parts[1])
        .databaseName(parts[0])
        .tableName(parts[1])
        .weight(weight)
        .sizeBytes(0L)
        .build();
  }
}
