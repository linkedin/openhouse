package com.linkedin.openhouse.datalayout.persistence;

import com.linkedin.openhouse.datalayout.strategy.RewriteStrategy;
import java.util.List;

/** DAO interface for persisting and loading data layout optimization strategies. */
public interface StrategiesDao {
  void save(String fqtn, List<RewriteStrategy> strategies);

  List<RewriteStrategy> load(String fqtn);
}
