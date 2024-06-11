package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import com.linkedin.openhouse.datalayout.datasource.ColumnStats;

/** Parent interface for any column frequency-based column order generation strategy. */
public interface FrequencyColumnOrderGenerator<T extends ColumnStats>
    extends ColumnOrderGenerator {}
