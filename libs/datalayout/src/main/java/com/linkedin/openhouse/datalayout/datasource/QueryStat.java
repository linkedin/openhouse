package com.linkedin.openhouse.datalayout.datasource;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class QueryStat {
  private final List<String> accessedColumns;
}
