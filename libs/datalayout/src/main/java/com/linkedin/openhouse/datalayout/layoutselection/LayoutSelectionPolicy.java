package com.linkedin.openhouse.datalayout.layoutselection;

public interface LayoutSelectionPolicy<T extends DataLayout> {
  // TODO: Determine what type this would need to be, do we want to rank?
  T evaluate();
}
