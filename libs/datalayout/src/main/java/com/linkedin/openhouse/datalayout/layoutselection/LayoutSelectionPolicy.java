package com.linkedin.openhouse.datalayout.layoutselection;

public interface LayoutSelectionPolicy {
  // TODO: Determine what type this would need to be, do we want to rank?
  void evaluate();
}
