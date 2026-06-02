package com.linkedin.openhouse.optimizer.scheduler.binpack;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import java.util.UUID;
import org.junit.jupiter.api.Test;

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

  private static TableStatsDto statsWithFiles(Long fileCount) {
    return TableStatsDto.builder()
        .snapshot(TableStatsDto.SnapshotMetrics.builder().numCurrentFiles(fileCount).build())
        .build();
  }

  @Test
  void fromOpAndStats_buildsFullyQualifiedNameAndOperationId() {
    TableOperationDto op = op();
    BinItem item = new TotalFilesBinItem().fromOpAndStats(op, statsWithFiles(42L));

    assertThat(item.getFullyQualifiedTableName()).isEqualTo("db1.tbl1");
    assertThat(item.getOperationId()).isEqualTo(op.getId());
  }

  @Test
  void fromOpAndStats_weightIsCurrentFileCount() {
    BinItem item = new TotalFilesBinItem().fromOpAndStats(op(), statsWithFiles(123_456L));
    assertThat(item.getWeight()).isEqualTo(123_456L);
  }

  @Test
  void fromOpAndStats_nullStats_weightIsZero() {
    BinItem item = new TotalFilesBinItem().fromOpAndStats(op(), null);
    assertThat(item.getWeight()).isEqualTo(0L);
  }

  @Test
  void fromOpAndStats_nullSnapshot_weightIsZero() {
    BinItem item = new TotalFilesBinItem().fromOpAndStats(op(), TableStatsDto.builder().build());
    assertThat(item.getWeight()).isEqualTo(0L);
  }

  @Test
  void fromOpAndStats_nullFileCount_weightIsZero() {
    BinItem item = new TotalFilesBinItem().fromOpAndStats(op(), statsWithFiles(null));
    assertThat(item.getWeight()).isEqualTo(0L);
  }

  @Test
  void emptyInstance_doesNotShareStateWithPopulated() {
    TotalFilesBinItem empty = new TotalFilesBinItem();
    BinItem populated = empty.fromOpAndStats(op(), statsWithFiles(7L));

    assertThat(empty.getWeight()).isEqualTo(0L);
    assertThat(populated.getWeight()).isEqualTo(7L);
  }
}
