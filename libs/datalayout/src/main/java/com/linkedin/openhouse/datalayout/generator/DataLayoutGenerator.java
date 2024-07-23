package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutOptimizationStrategy;
import java.util.List;

public interface DataLayoutGenerator {
  List<DataLayoutOptimizationStrategy> generate();
}
