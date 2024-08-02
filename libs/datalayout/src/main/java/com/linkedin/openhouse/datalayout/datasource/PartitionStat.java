package com.linkedin.openhouse.datalayout.datasource;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Represents table partition stats. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionStat {
  // list of transformed values for a given table partition
  // e.g. if table is partitioned by (datepartition: string, state: string)
  // the list could be ["2024-01-01", "CA"]
  private List<String> values;
  private int fileCount;
}
