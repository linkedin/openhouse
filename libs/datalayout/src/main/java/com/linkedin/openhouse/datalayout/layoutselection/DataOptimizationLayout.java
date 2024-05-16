package com.linkedin.openhouse.datalayout.layoutselection;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DataOptimizationLayout implements DataLayout {
  private final double score;
  private final DataCompactionConfig config;
  // TODO: support sorting config
}
