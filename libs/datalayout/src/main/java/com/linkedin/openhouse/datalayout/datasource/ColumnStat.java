package com.linkedin.openhouse.datalayout.datasource;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ColumnStat {
  private final String name;
  private final long accessCount;
}
