package com.linkedin.openhouse.datalayout.detection;

import com.linkedin.openhouse.datalayout.datasource.FileStat;
import com.linkedin.openhouse.datalayout.datasource.TableFileStats;
import com.linkedin.openhouse.datalayout.layoutselection.DataCompactionLayout;
import lombok.Builder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Encoders;

/**
 * Trigger that computes the deviation of a set of files from a target file size. Will return 'true'
 * if a threshold is exceeded.
 */
@Builder
public class FileEntropyTrigger
    implements DataCompactionTrigger<FileStat, DataCompactionLayout, TableFileStats> {
  private final DataCompactionLayout targetLayout;
  private final TableFileStats tableFileStats;
  private final double threshold;

  /** Compute the file entropy as the difference between the target and actual file size(s). */
  private double computeEntropy() {
    long numFiles = tableFileStats.get().count();
    long targetFileSize = targetLayout.getTargetSizeBytes();
    if (numFiles == 0) {
      throw new IllegalStateException("Table must contain at least one file.");
    }
    if (targetFileSize <= 0) {
      throw new IllegalArgumentException(
          "Target file size has to be positive, current file size: " + targetFileSize);
    }

    // Compute mean file entropy.
    return tableFileStats
            .get()
            .map(
                (MapFunction<FileStat, Double>)
                    fs -> Math.pow(((double) fs.getSize() - targetFileSize) / 1000000.0, 2.0),
                Encoders.DOUBLE())
            .reduce((ReduceFunction<Double>) Double::sum)
        / numFiles;
  }

  @Override
  public boolean check() {
    return computeEntropy() >= threshold;
  }
}
