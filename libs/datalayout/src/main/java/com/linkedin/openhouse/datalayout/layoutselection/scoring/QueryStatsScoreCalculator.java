package com.linkedin.openhouse.datalayout.layoutselection.scoring;

import com.linkedin.openhouse.datalayout.datasource.QueryStats;

/** Parent interface for any query stats based score calculation. */
public interface QueryStatsScoreCalculator<T extends QueryStats>
    extends ScoreCalculator {}
