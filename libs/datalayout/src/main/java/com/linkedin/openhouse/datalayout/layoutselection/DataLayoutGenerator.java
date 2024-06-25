package com.linkedin.openhouse.datalayout.layoutselection;

import java.util.List;

public interface DataLayoutGenerator {
  List<DataLayoutOptimizationStrategy> generate();
}
