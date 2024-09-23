package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
public class TableDataLayoutMetadata extends TableMetadata {
  protected DataLayoutStrategy dataLayoutStrategy;
}
