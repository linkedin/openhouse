package com.linkedin.openhouse.jobs.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataFilesSummary {

  private Integer content;
  private Long sumOfFileSizeBytes;
  private Long totalFileCount;

  /**
   * Returns the Spark Encoder for this class.
   *
   * @return Encoder for ExampleData
   */
  public static Encoder<DataFilesSummary> getEncoder() {
    return Encoders.bean(DataFilesSummary.class);
  }
}
