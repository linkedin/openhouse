package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Encoders;

@Builder
public class OpenHouseLayoutSelectionPolicy implements LayoutSelectionPolicy<DataCompactionLayout> {
  private final TableFileStats tableFileStats;

  @Override
  public DataCompactionLayout evaluate() {
    long totalSize =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .reduce((ReduceFunction<Long>) Long::sum);
    if (totalSize > 1024L * 1024L * 1024L * 1024L * 100L) {
      return DataCompactionLayout.builder().targetSizeBytes(1024 * 1024 * 1024L * 2).build();
    }
    return DataCompactionLayout.builder().targetSizeBytes(1024 * 1024 * 512L).build();
  }
}
