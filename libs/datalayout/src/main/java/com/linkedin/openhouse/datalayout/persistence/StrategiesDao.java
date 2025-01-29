package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.List;

/** DAO interface for persisting and loading data layout optimization strategies. */
public interface StrategiesDao {
  void save(String fqtn, String key, List<DataLayoutStrategy> strategies);

  List<DataLayoutStrategy> load(String fqtn, String key);
}
