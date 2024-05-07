package com.linkedin.openhouse.datalayout.layoutselection;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DataCompactionLayout implements DataLayout {
  public static final long TARGET_SIZE_BYTES_DEFAULT = 512 * 1024 * 1024; // 512 MB
  private final long targetSizeBytes;
}
