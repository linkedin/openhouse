package com.linkedin.openhouse.datalayout.datasource;

import java.io.Serializable;
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
public class PartitionStat implements Serializable {
  private List<String> values;
  private int fileCount;
}
