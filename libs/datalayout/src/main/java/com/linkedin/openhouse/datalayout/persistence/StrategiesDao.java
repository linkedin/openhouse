package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.layoutselection.DataLayoutOptimizationStrategy;
import java.util.List;

public interface StrategiesDao {
  void save(String fqtn, List<DataLayoutOptimizationStrategy> strategies);

  List<DataLayoutOptimizationStrategy> load(String fqtn);
}
