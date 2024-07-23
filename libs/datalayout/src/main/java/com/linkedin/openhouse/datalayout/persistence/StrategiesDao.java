package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutOptimizationStrategy;
import java.util.List;

/** DAO interface for persisting and loading data layout optimization strategies. */
public interface StrategiesDao {
  void save(String fqtn, List<DataLayoutOptimizationStrategy> strategies);

  List<DataLayoutOptimizationStrategy> load(String fqtn);
}
