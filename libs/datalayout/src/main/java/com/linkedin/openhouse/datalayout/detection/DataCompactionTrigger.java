package com.linkedin.openhouse.datalayout.detection;

import com.linkedin.openhouse.datalayout.datasource.DataSource;
import com.linkedin.openhouse.datalayout.layoutselection.DataCompactionLayout;

/**
 * Determines if a table needs to be compacted via the function 'evaluate'. The implementation of
 * the decision function is dependent on the policy.
 */
public interface DataCompactionTrigger<D, T extends DataCompactionLayout, S extends DataSource<D>>
    extends LayoutOptimizationTrigger {}
