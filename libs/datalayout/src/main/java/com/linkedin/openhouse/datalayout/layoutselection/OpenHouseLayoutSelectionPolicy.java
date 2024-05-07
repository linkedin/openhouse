package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Encoders;

@Builder
public class OpenHouseLayoutSelectionPolicy implements LayoutSelectionPolicy<DataCompactionLayout> {
  private static final long LARGE_TABLE_THRESHOLD_BYTES = 1024L * 1024L * 1024L * 100L;
  private final TableFileStats tableFileStats;

  @Override
  public DataCompactionLayout evaluate() {
    long totalSize =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .reduce((ReduceFunction<Long>) Long::sum);
    if (totalSize > LARGE_TABLE_THRESHOLD_BYTES) {
      return DataCompactionLayout.builder()
          .targetSizeBytes(DataCompactionLayout.TARGET_SIZE_BYTES_DEFAULT * 4)
          .build();
    }
    return DataCompactionLayout.builder()
        .targetSizeBytes(DataCompactionLayout.TARGET_SIZE_BYTES_DEFAULT)
        .build();
  }
}
