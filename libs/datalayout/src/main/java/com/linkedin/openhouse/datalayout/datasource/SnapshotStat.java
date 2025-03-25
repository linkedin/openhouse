package com.linkedin.openhouse.datalayout.datasource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotStat {
  long committedAt;
  long snapshotId;
  String operation;
}
