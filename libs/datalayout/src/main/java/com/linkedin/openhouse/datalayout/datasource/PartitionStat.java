package com.linkedin.openhouse.datalayout.datasource;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Represents table partition. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionStat {
  private List<String> values;
  private int fileCount;
}
