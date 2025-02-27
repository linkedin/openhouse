package com.linkedin.openhouse.datalayout.datasource;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iceberg.FileContent;

/** Represents the statistics of a file. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class FileStat {
  private FileContent content;
  private String path;
  private long sizeInBytes;
  private long recordCount;
  private List<String> partitionValues;
}
