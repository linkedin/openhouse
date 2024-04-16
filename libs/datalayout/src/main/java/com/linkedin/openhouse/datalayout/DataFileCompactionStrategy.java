package com.linkedin.openhouse.datalayout;

import lombok.Builder;

@Builder
public class DataFileCompactionStrategy implements DataLayoutOptimizationStrategy {
  private final long targetByteSize;
}
