package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Encoders;

@Builder
public class OpenHouseLayoutSelectionPolicy
    implements LayoutSelectionPolicy<DataOptimizationLayout> {
  private static final long FILE_BLOCK_SIZE_BYTES = 1024L * 1024L * 256;
  private static final long FILE_BLOCK_MARGIN_BYTES = 1024L * 1024L * 10;
  private final TableFileStats tableFileStats;

  @Override
  public List<DataOptimizationLayout> evaluate() {
    long totalSize =
        tableFileStats
            .get()
            .map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG())
            .reduce((ReduceFunction<Long>) Long::sum);

    // TODO: make use of stats to determine the app configuration
    DataCompactionConfig.DataCompactionConfigBuilder configBuilder = DataCompactionConfig.builder();
    // Make sure the last block is almost full
    configBuilder.targetByteSize(2 * FILE_BLOCK_SIZE_BYTES - FILE_BLOCK_MARGIN_BYTES);
    return Collections.singletonList(
        DataOptimizationLayout.builder().config(configBuilder.build()).score(1.0).build());
  }
}
