package com.linkedin.openhouse.datalayout.datasource;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class FileStat {
  private final String path;
  private final long size;
}
