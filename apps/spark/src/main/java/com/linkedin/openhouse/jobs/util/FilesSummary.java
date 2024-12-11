package com.linkedin.openhouse.jobs.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * Represents a summary of data files, including the content, total size, and total count of files.
 * This class can be used to store and manipulate summary data for collections of files, and
 * provides a encoder for use with serializing Spark typed datasets
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FilesSummary {

  private Integer content;
  private Long sumOfFileSizeBytes;
  private Long totalFileCount;

  /**
   * Returns the Spark Encoder for this class using shared object and thread-safe
   * initialization-on-demand holder idiom.
   *
   * @return Encoder for DataFilesSummary
   */
  private static class EncoderSingleton {
    public static final Encoder<FilesSummary> instance = Encoders.bean(FilesSummary.class);
  }

  public static Encoder<FilesSummary> getEncoder() {
    return EncoderSingleton.instance;
  }
}
