package com.linkedin.openhouse.datalayout.generator;

import com.linkedin.openhouse.datalayout.strategy.RewriteStrategy;
import java.util.List;

public interface RewriteStrategyGenerator {
  List<RewriteStrategy> generate();
}
