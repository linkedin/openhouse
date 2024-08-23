package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.datasource.TablePartitionStats;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.util.DataCompactionConfigUtil;
import com.linkedin.openhouse.datalayout.util.TableFileStatsUtil;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

/**
 * Data layout optimization strategies generator for OpenHouse. Generates a list of strategies with
 * score.
 */
@Builder
public class FileEntropyDataLayoutStrategyGenerator implements DataLayoutStrategyGenerator {
  private static final long MB = 1024 * 1024;
  private static final long FILE_BLOCK_SIZE_BYTES = 256 * MB;
  private static final long FILE_BLOCK_MARGIN_BYTES = 10 * MB;
  private static final long TARGET_BYTES_SIZE = 2 * FILE_BLOCK_SIZE_BYTES - FILE_BLOCK_MARGIN_BYTES;
  private final TableFileStats tableFileStats;
  private final TablePartitionStats tablePartitionStats;

  /**
   * Generate a list of data layout optimization strategies based on the table file stats and
   * historic query patterns.
   */
  @Override
  public List<DataLayoutStrategy> generate() {
    return Collections.singletonList(generateCompactionStrategy());
  }

  /**
   * Computes the score for a partitioned table as file entropy, the score is the mean-squared
   * error. The higher the MSE, the worse the table state.
   */
  private DataLayoutStrategy generateCompactionStrategy() {

    // Retrieve file sizes of all data files.
    Dataset<Long> fileSizes =
        tableFileStats.get().map((MapFunction<FileStat, Long>) FileStat::getSize, Encoders.LONG());

    // Aggregate MSE values.
    Double mse = 0.0;
    Iterator<Long> itr = fileSizes.toLocalIterator();
    while (itr.hasNext()) {
      mse += Math.pow(TARGET_BYTES_SIZE - itr.next(), 2);
    }
    // Normalize.
    mse = mse / fileSizes.count();

    return DataLayoutStrategy.builder()
        .config(
            DataCompactionConfigUtil.createDefaultCompactionConfig(
                TARGET_BYTES_SIZE,
                tablePartitionStats.get().count(),
                TableFileStatsUtil.getAggregatedFileStats(tableFileStats, TARGET_BYTES_SIZE)._1))
        .score(mse)
        .build();
  }
}
