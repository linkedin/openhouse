package com.linkedin.openhouse.datalayout.layoutselection;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DataCompactionLayout implements DataLayout {
  private final long targetSizeBytes;
}
