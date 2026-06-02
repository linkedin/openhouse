package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;

class FirstFitBinPackerTest {

  private static final OperationTypeDto TYPE = OperationTypeDto.ORPHAN_FILES_DELETION;

  @AllArgsConstructor
  @Getter
  static class TestItem implements BinItem {
    private final String id;
    private final long weight;

    @Override
    public String getFullyQualifiedTableName() {
      return "db.tbl_" + id;
    }

    @Override
    public String getOperationId() {
      return "op-" + id;
    }

    @Override
    public BinItem withOpAndStats(TableOperationDto op, TableStatsDto stats) {
      throw new UnsupportedOperationException("test items are not used as prototypes");
    }
  }

  private static TestItem item(String id, long weight) {
    return new TestItem(id, weight);
  }

  @Test
  void emptyInput_returnsEmptyBins() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 100L, 10);
    assertThat(packer.pack(List.of())).isEmpty();
  }

  @Test
  void singleItem_oneBin() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 1_000_000L, 10);
    List<Bin> bins = packer.pack(List.of(item("a", 100L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getItems()).hasSize(1);
    assertThat(bins.get(0).getOperationType()).isEqualTo(TYPE);
  }

  @Test
  void underWeightLimit_oneBin() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 1_000_000L, 10);
    List<Bin> bins =
        packer.pack(List.of(item("a", 300_000L), item("b", 300_000L), item("c", 300_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getItems()).hasSize(3);
  }

  @Test
  void overWeightLimit_twoBins() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 1_000_000L, 10);
    List<Bin> bins =
        packer.pack(List.of(item("a", 600_000L), item("b", 600_000L), item("c", 400_000L)));
    assertThat(bins).hasSize(2);
    // FFD: sort desc → 600, 600, 400. Place 600 → bin0; next 600 doesn't fit bin0, → bin1; 400
    // fits bin0 (total 1_000_000).
    long b0 = bins.get(0).getItems().stream().mapToLong(BinItem::getWeight).sum();
    long b1 = bins.get(1).getItems().stream().mapToLong(BinItem::getWeight).sum();
    assertThat(b0).isEqualTo(1_000_000L);
    assertThat(b1).isEqualTo(600_000L);
  }

  @Test
  void itemLargerThanCap_getsOwnBin() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 1_000L, 10);
    List<Bin> bins = packer.pack(List.of(item("big", 5_000L)));
    assertThat(bins).hasSize(1);
    assertThat(bins.get(0).getItems()).hasSize(1);
  }

  @Test
  void sortedDescending_largestFirst() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 2_000_000L, 10);
    List<Bin> bins = packer.pack(List.of(item("small", 100L), item("large", 900_000L)));
    assertThat(bins).hasSize(1);
    List<String> ids =
        bins.get(0).getItems().stream()
            .map(TestItem.class::cast)
            .map(TestItem::getId)
            .collect(Collectors.toList());
    assertThat(ids).containsExactly("large", "small");
  }

  @Test
  void maxItemsCap_splitsBins() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 1_000_000L, 2);
    List<Bin> bins =
        packer.pack(List.of(item("a", 1L), item("b", 1L), item("c", 1L), item("d", 1L)));
    assertThat(bins).hasSize(2);
    assertThat(bins.get(0).getItems()).hasSize(2);
    assertThat(bins.get(1).getItems()).hasSize(2);
  }

  @Test
  void binsCarryConfiguredOperationType() {
    FirstFitBinPacker packer = new FirstFitBinPacker(TYPE, 100L, 10);
    List<Bin> bins = packer.pack(List.of(item("a", 1L)));
    assertThat(bins.get(0).getOperationType()).isEqualTo(TYPE);
  }
}
