package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Covers the projection that {@link TotalFilesFirstFitBinPacker} applies when constructing {@link
 * TotalFilesBinItem}s — fully-qualified name, operation id, and weight derived from the snapshot's
 * current file count, with the null-safety chain that handles missing snapshot fields.
 */
class TotalFilesBinItemTest {

  private static TableOperationDto op() {
    return TableOperationDto.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(UUID.randomUUID().toString())
        .databaseName("db1")
        .tableName("tbl1")
        .operationType(OperationTypeDto.ORPHAN_FILES_DELETION)
        .build();
  }

  private static TableStatsDto statsWithFiles(String uuid, Long fileCount) {
    return TableStatsDto.builder()
        .tableUuid(uuid)
        .snapshot(TableStatsDto.SnapshotMetrics.builder().numCurrentFiles(fileCount).build())
        .build();
  }

  private static List<BinItem> pack(TableOperationDto op, TableStatsDto stats) {
    TotalFilesFirstFitBinPacker packer =
        new TotalFilesFirstFitBinPacker(Long.MAX_VALUE, Integer.MAX_VALUE);
    List<List<BinItem>> groupings = packer.pack(List.of(op), Map.of(op.getTableUuid(), stats));
    assertThat(groupings).hasSize(1);
    return groupings.get(0);
  }

  @Test
  void projectionBuildsFullyQualifiedNameAndOperationId() {
    TableOperationDto op = op();
    List<BinItem> items = pack(op, statsWithFiles(op.getTableUuid(), 42L));

    assertThat(items).hasSize(1);
    assertThat(items.get(0).getFullyQualifiedTableName()).isEqualTo("db1.tbl1");
    assertThat(items.get(0).getOperationId()).isEqualTo(op.getId());
  }

  @Test
  void weightIsCurrentFileCount() {
    TableOperationDto op = op();
    List<BinItem> items = pack(op, statsWithFiles(op.getTableUuid(), 123_456L));
    assertThat(items.get(0).getWeight()).isEqualTo(123_456L);
  }

  @Test
  void nullSnapshotFields_weightIsZero() {
    TableOperationDto op = op();
    TableStatsDto emptySnapshot = TableStatsDto.builder().tableUuid(op.getTableUuid()).build();
    List<BinItem> items = pack(op, emptySnapshot);
    assertThat(items.get(0).getWeight()).isEqualTo(0L);
  }

  @Test
  void nullFileCount_weightIsZero() {
    TableOperationDto op = op();
    List<BinItem> items = pack(op, statsWithFiles(op.getTableUuid(), null));
    assertThat(items.get(0).getWeight()).isEqualTo(0L);
  }
}
