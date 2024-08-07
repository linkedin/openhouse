package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.List;

public interface DataLayoutStrategyGenerator {
  List<DataLayoutStrategy> generate();
}
