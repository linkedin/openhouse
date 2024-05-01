package com.linkedin.openhouse.datalayout.datamaintenance;

import java.util.List;

/**
 * Policy that computes the deviation of a set of files from a target file size. Will return 'true'
 * if a threshold is exceeded.
 */
public class FileEntropyPolicy implements CompactionPolicy {

  private List<Integer> fileSizes;
  private Integer targetFileSize;
  Double threshold;

  public FileEntropyPolicy(List<Integer> fileSizes, Integer targetFileSize, Double threshold) {
    this.fileSizes = fileSizes;
    this.targetFileSize = targetFileSize;
    this.threshold = threshold;
  }

  public boolean compact() {
    return computeEntropy() >= threshold;
  }

  /** Compute the file entropy as the difference between the target and actual file size(s). */
  private Double computeEntropy() {
    if (this.fileSizes.size() == 0) {
      throw new IllegalArgumentException("Table must contain at least one file.");
    }
    if (this.targetFileSize > 0) {
      throw new IllegalArgumentException(
          "Target file size has to be positive, current file size: " + this.targetFileSize);
    }

    // Compute mean file entropy.
    Double result = 0.0;
    for (Integer fileSize : this.fileSizes) {
      result += Math.pow(fileSize - this.targetFileSize, 2);
    }
    return result / this.fileSizes.size();
  }
}
